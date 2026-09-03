// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Local Cloudflare Images binding (`env.IMAGES`).
//!
//! Transforms run in-process with the `image` crate. celld does not call
//! Cloudflare's paid Images API. `@img/sharp-wasm32` is not loaded into the
//! isolate: the wasm/WASI payload is several megabytes and would sit inside
//! every worker heap. The host object still matches the Workers
//! `IMAGES.input().transform().output()` shape.

use anyhow::{anyhow, bail, Context};
use image::imageops::FilterType;
use image::{DynamicImage, GenericImageView, ImageEncoder, ImageFormat, ImageReader};
use serde::Deserialize;
use serde_json::{json, Value};
use std::io::Cursor;

/// Bytes one Images request may decode. Larger inputs fail closed.
pub const MAX_INPUT_BYTES: usize = 32 * 1024 * 1024;

/// Longest edge after a resize. Cloudflare Images documents 12,000.
const MAX_DIMENSION: u32 = 12_000;

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Transform {
    #[serde(default)]
    width: Option<u32>,
    #[serde(default)]
    height: Option<u32>,
    #[serde(default)]
    fit: Option<String>,
    #[serde(default)]
    gravity: Option<Gravity>,
    #[serde(default)]
    background: Option<String>,
    #[serde(default)]
    rotate: Option<i32>,
    #[serde(default)]
    flip: Option<bool>,
    #[serde(default)]
    flop: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Gravity {
    Named(String),
    Point { x: f64, y: f64 },
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Output {
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    quality: Option<u8>,
}

/// Decode + transform + encode. `transforms` and `output` are JSON objects
/// the harness already validated as objects (unknown keys still fail here).
pub fn process(input: &[u8], transforms: &[Value], output: &Value) -> anyhow::Result<Vec<u8>> {
    if input.len() > MAX_INPUT_BYTES {
        bail!(
            "Images input is {} bytes; celld accepts at most {MAX_INPUT_BYTES}",
            input.len()
        );
    }
    if input.is_empty() {
        bail!("Images input is empty");
    }
    let mut image = load(input)?;
    for transform in transforms {
        image = apply_transform(image, transform)?;
    }
    encode(&image, output)
}

/// Metadata only. `info()` does not apply transforms.
pub fn info(input: &[u8]) -> anyhow::Result<Value> {
    if input.len() > MAX_INPUT_BYTES {
        bail!(
            "Images input is {} bytes; celld accepts at most {MAX_INPUT_BYTES}",
            input.len()
        );
    }
    if input.is_empty() {
        bail!("Images input is empty");
    }
    let reader = ImageReader::new(Cursor::new(input))
        .with_guessed_format()
        .context("Images could not read the input")?;
    let format = reader.format();
    let decoded = reader.decode().context("Images could not decode the input")?;
    let (width, height) = decoded.dimensions();
    Ok(json!({
        "format": format_name(format),
        "width": width,
        "height": height,
    }))
}

fn load(input: &[u8]) -> anyhow::Result<DynamicImage> {
    ImageReader::new(Cursor::new(input))
        .with_guessed_format()
        .context("Images could not read the input")?
        .decode()
        .context("Images could not decode the input")
}

fn apply_transform(mut image: DynamicImage, value: &Value) -> anyhow::Result<DynamicImage> {
    let transform: Transform = serde_json::from_value(value.clone())
        .map_err(|error| anyhow!("Images.transform() is not supported: {error}"))?;
    if let Some(degrees) = transform.rotate {
        image = match degrees.rem_euclid(360) {
            0 => image,
            90 => image.rotate90(),
            180 => image.rotate180(),
            270 => image.rotate270(),
            other => bail!("Images.transform() rotate must be a multiple of 90, not {other}"),
        };
    }
    if transform.flip.unwrap_or(false) {
        image = image.flipv();
    }
    if transform.flop.unwrap_or(false) {
        image = image.fliph();
    }
    let (src_w, src_h) = image.dimensions();
    let width = transform.width.filter(|value| *value > 0);
    let height = transform.height.filter(|value| *value > 0);
    if width.is_none() && height.is_none() {
        return Ok(image);
    }
    let fit = transform.fit.as_deref().unwrap_or("scale-down");
    let (dst_w, dst_h) = target_size(src_w, src_h, width, height, fit)?;
    if dst_w > MAX_DIMENSION || dst_h > MAX_DIMENSION {
        bail!("Images.transform() result exceeds {MAX_DIMENSION}px on an edge");
    }
    Ok(match fit {
        "scale-down" | "contain" | "pad" | "squeeze" => {
            if dst_w == src_w && dst_h == src_h && fit != "pad" {
                image
            } else {
                let resized = image.resize_exact(dst_w, dst_h, FilterType::Triangle);
                if fit == "pad" {
                    pad(
                        resized,
                        width.unwrap_or(dst_w),
                        height.unwrap_or(dst_h),
                        transform.background.as_deref(),
                    )?
                } else {
                    resized
                }
            }
        }
        "cover" | "crop" => crop_cover(
            image,
            dst_w,
            dst_h,
            transform.gravity.as_ref(),
        )?,
        other => bail!("Images.transform() fit {other:?} is not supported"),
    })
}

fn target_size(
    src_w: u32,
    src_h: u32,
    width: Option<u32>,
    height: Option<u32>,
    fit: &str,
) -> anyhow::Result<(u32, u32)> {
    match fit {
        "squeeze" => Ok((width.unwrap_or(src_w), height.unwrap_or(src_h))),
        "scale-down" => {
            let (w, h) = contain_size(src_w, src_h, width, height);
            Ok((w.min(src_w), h.min(src_h)))
        }
        "contain" | "pad" => Ok(contain_size(src_w, src_h, width, height)),
        "cover" | "crop" => Ok((
            width.unwrap_or(src_w),
            height.unwrap_or(src_h),
        )),
        other => bail!("Images.transform() fit {other:?} is not supported"),
    }
}

fn contain_size(src_w: u32, src_h: u32, width: Option<u32>, height: Option<u32>) -> (u32, u32) {
    let src_w = src_w.max(1) as f64;
    let src_h = src_h.max(1) as f64;
    match (width, height) {
        (Some(w), Some(h)) => {
            let scale = (w as f64 / src_w).min(h as f64 / src_h);
            (
                ((src_w * scale).round() as u32).max(1),
                ((src_h * scale).round() as u32).max(1),
            )
        }
        (Some(w), None) => {
            let scale = w as f64 / src_w;
            (w, ((src_h * scale).round() as u32).max(1))
        }
        (None, Some(h)) => {
            let scale = h as f64 / src_h;
            (((src_w * scale).round() as u32).max(1), h)
        }
        (None, None) => (src_w as u32, src_h as u32),
    }
}

fn crop_cover(
    image: DynamicImage,
    dst_w: u32,
    dst_h: u32,
    gravity: Option<&Gravity>,
) -> anyhow::Result<DynamicImage> {
    let (src_w, src_h) = image.dimensions();
    let scale = (dst_w as f64 / src_w.max(1) as f64).max(dst_h as f64 / src_h.max(1) as f64);
    let scaled_w = ((src_w as f64 * scale).ceil() as u32).max(dst_w);
    let scaled_h = ((src_h as f64 * scale).ceil() as u32).max(dst_h);
    let scaled = if scaled_w == src_w && scaled_h == src_h {
        image
    } else {
        image.resize_exact(scaled_w, scaled_h, FilterType::Triangle)
    };
    let (left, top) = crop_origin(scaled_w, scaled_h, dst_w, dst_h, gravity)?;
    Ok(scaled.crop_imm(left, top, dst_w, dst_h))
}

fn crop_origin(
    src_w: u32,
    src_h: u32,
    dst_w: u32,
    dst_h: u32,
    gravity: Option<&Gravity>,
) -> anyhow::Result<(u32, u32)> {
    let max_x = src_w.saturating_sub(dst_w);
    let max_y = src_h.saturating_sub(dst_h);
    match gravity {
        None => Ok((max_x / 2, max_y / 2)),
        Some(Gravity::Named(name)) => {
            let (fx, fy) = named_gravity(name)?;
            Ok((
                ((max_x as f64) * fx).round() as u32,
                ((max_y as f64) * fy).round() as u32,
            ))
        }
        Some(Gravity::Point { x, y }) => {
            if !(0.0..=1.0).contains(x) || !(0.0..=1.0).contains(y) {
                bail!("Images.transform() gravity coordinates must be in [0, 1]");
            }
            Ok((
                ((max_x as f64) * x).round() as u32,
                ((max_y as f64) * y).round() as u32,
            ))
        }
    }
}

fn named_gravity(name: &str) -> anyhow::Result<(f64, f64)> {
    Ok(match name {
        "center" | "auto" => (0.5, 0.5),
        "top" | "north" => (0.5, 0.0),
        "bottom" | "south" => (0.5, 1.0),
        "left" | "west" => (0.0, 0.5),
        "right" | "east" => (1.0, 0.5),
        "top-left" | "northwest" => (0.0, 0.0),
        "top-right" | "northeast" => (1.0, 0.0),
        "bottom-left" | "southwest" => (0.0, 1.0),
        "bottom-right" | "southeast" => (1.0, 1.0),
        other => bail!("Images.transform() gravity {other:?} is not supported"),
    })
}

fn pad(
    image: DynamicImage,
    canvas_w: u32,
    canvas_h: u32,
    background: Option<&str>,
) -> anyhow::Result<DynamicImage> {
    let (src_w, src_h) = image.dimensions();
    if src_w >= canvas_w && src_h >= canvas_h {
        return Ok(image);
    }
    let color = parse_background(background.unwrap_or("#000000"))?;
    let mut canvas = DynamicImage::ImageRgba8(image::RgbaImage::from_pixel(canvas_w, canvas_h, color));
    let left = canvas_w.saturating_sub(src_w) / 2;
    let top = canvas_h.saturating_sub(src_h) / 2;
    image::imageops::overlay(&mut canvas, &image, left.into(), top.into());
    Ok(canvas)
}

fn parse_background(value: &str) -> anyhow::Result<image::Rgba<u8>> {
    let hex = value.strip_prefix('#').unwrap_or(value);
    let digits = hex.as_bytes();
    let parse = |start: usize, len: usize| -> anyhow::Result<u8> {
        u8::from_str_radix(
            std::str::from_utf8(&digits[start..start + len]).unwrap_or(""),
            16,
        )
        .map_err(|_| anyhow!("Images.transform() background {value:?} is not a hex color"))
    };
    match hex.len() {
        3 => Ok(image::Rgba([
            parse(0, 1)? * 17,
            parse(1, 1)? * 17,
            parse(2, 1)? * 17,
            255,
        ])),
        6 => Ok(image::Rgba([parse(0, 2)?, parse(2, 2)?, parse(4, 2)?, 255])),
        8 => Ok(image::Rgba([
            parse(0, 2)?,
            parse(2, 2)?,
            parse(4, 2)?,
            parse(6, 2)?,
        ])),
        _ => bail!("Images.transform() background {value:?} is not a hex color"),
    }
}

fn encode(image: &DynamicImage, output: &Value) -> anyhow::Result<Vec<u8>> {
    let output: Output = serde_json::from_value(output.clone())
        .map_err(|error| anyhow!("Images.output() is not supported: {error}"))?;
    let format = match output.format.as_deref() {
        None | Some("image/jpeg") | Some("jpeg") | Some("jpg") => ImageFormat::Jpeg,
        Some("image/png") | Some("png") => ImageFormat::Png,
        Some("image/webp") | Some("webp") => ImageFormat::WebP,
        Some("image/avif") | Some("avif") => ImageFormat::Avif,
        Some(other) => bail!("Images.output() format {other:?} is not supported"),
    };
    if let Some(quality) = output.quality {
        if quality == 0 || quality > 100 {
            bail!("Images.output() quality must be between 1 and 100");
        }
        if !matches!(format, ImageFormat::Jpeg | ImageFormat::WebP) {
            bail!("Images.output() quality applies only to jpeg and webp");
        }
    }
    let mut bytes = Vec::new();
    let cursor = Cursor::new(&mut bytes);
    match format {
        ImageFormat::Jpeg => {
            let quality = output.quality.unwrap_or(85);
            let encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(cursor, quality);
            encoder
                .write_image(
                    image.to_rgb8().as_raw(),
                    image.width(),
                    image.height(),
                    image::ExtendedColorType::Rgb8,
                )
                .context("Images could not encode JPEG")?;
        }
        ImageFormat::Png => {
            image
                .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
                .context("Images could not encode PNG")?;
        }
        ImageFormat::WebP => {
            image
                .write_to(&mut Cursor::new(&mut bytes), ImageFormat::WebP)
                .context("Images could not encode WebP")?;
        }
        ImageFormat::Avif => {
            image
                .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Avif)
                .context("Images could not encode AVIF")?;
        }
        _ => unreachable!("encode format is validated above"),
    }
    Ok(bytes)
}

fn format_name(format: Option<ImageFormat>) -> &'static str {
    match format {
        Some(ImageFormat::Jpeg) => "image/jpeg",
        Some(ImageFormat::Png) => "image/png",
        Some(ImageFormat::WebP) => "image/webp",
        Some(ImageFormat::Avif) => "image/avif",
        Some(ImageFormat::Gif) => "image/gif",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use image::{ImageBuffer, Rgba};

    fn png_bytes(width: u32, height: u32) -> Vec<u8> {
        let image = DynamicImage::ImageRgba8(ImageBuffer::from_fn(width, height, |x, y| {
            Rgba([
                (x * 17) as u8,
                (y * 17) as u8,
                80,
                255,
            ])
        }));
        let mut bytes = Vec::new();
        image
            .write_to(&mut Cursor::new(&mut bytes), ImageFormat::Png)
            .unwrap();
        bytes
    }

    #[test]
    fn resize_png_reports_new_size() {
        let input = png_bytes(32, 16);
        let out = process(
            &input,
            &[json!({"width": 8, "height": 8, "fit": "cover"})],
            &json!({"format": "image/png"}),
        )
        .unwrap();
        let meta = info(&out).unwrap();
        assert_eq!(meta["width"], 8);
        assert_eq!(meta["height"], 8);
        assert_eq!(meta["format"], "image/png");
    }

    #[test]
    fn unknown_transform_field_fails_closed() {
        let input = png_bytes(8, 8);
        let error = process(
            &input,
            &[json!({"width": 4, "sharpen": 1})],
            &json!({"format": "image/png"}),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("not supported"), "{error}");
    }

    #[test]
    fn info_reads_jpeg() {
        let input = png_bytes(10, 6);
        let jpeg = process(&input, &[], &json!({"format": "image/jpeg", "quality": 70})).unwrap();
        let meta = info(&jpeg).unwrap();
        assert_eq!(meta["format"], "image/jpeg");
        assert_eq!(meta["width"], 10);
        assert_eq!(meta["height"], 6);
    }

    #[test]
    fn encode_webp_roundtrip() {
        let input = png_bytes(8, 8);
        let webp = process(&input, &[], &json!({"format": "image/webp"})).unwrap();
        let meta = info(&webp).unwrap();
        assert_eq!(meta["format"], "image/webp");
    }
}
