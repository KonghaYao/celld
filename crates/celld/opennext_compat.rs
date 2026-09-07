//! OpenNext / Next-on-Workers URL compat at the celld ingress/egress boundary.
//!
//! Replaces part of `dev/examples/support-opennext/prepare-artifact.sh` bundle patches
//! (S30): absolute `request.url` slash normalization and `Location` shaping.

/// Collapse repeated slashes in an absolute `request.url` while preserving query.
pub fn normalize_worker_request_url(url: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(url) else {
        return url.to_string();
    };
    let path = parsed.path();
    let collapsed = collapse_slashes_in_path(path);
    if collapsed == path {
        return url.to_string();
    }
    parsed.set_path(&collapsed);
    parsed.to_string()
}

fn collapse_slashes_in_path(path: &str) -> String {
    let (suffix, query) = match path.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (path, None),
    };
    let normalized = suffix.replace('\\', "/");
    let segments: Vec<&str> = normalized.split('/').filter(|s| !s.is_empty()).collect();
    let mut out = if path.starts_with('/') {
        format!("/{}", segments.join("/"))
    } else {
        segments.join("/")
    };
    if out.is_empty() {
        out = "/".to_string();
    }
    if let Some(q) = query {
        out.push('?');
        out.push_str(q);
    }
    out
}

/// Port of OpenNext `normalizeLocationHeader` (same-origin → relative path + query).
pub fn normalize_worker_location(location: &str, request_url: &str) -> String {
    match location {
        "" | "?" | "//" => return "/".to_string(),
        _ => {}
    }
    let Ok(base) = url::Url::parse(request_url) else {
        return location.to_string();
    };
    let location_url = match url::Url::parse(location) {
        Ok(parsed) => parsed,
        Err(_) => match base.join(location) {
            Ok(joined) => joined,
            Err(_) => return location.to_string(),
        },
    };
    same_origin_relative_location(&location_url, &base)
}

fn same_origin_relative_location(location: &url::Url, base: &url::Url) -> String {
    if location.origin() != base.origin() {
        return location.to_string();
    }
    let path = location.path();
    let path = if path.is_empty() { "/" } else { path };
    let mut rel = format!(
        "{}{}",
        path,
        location
            .query()
            .map(|q| format!("?{q}"))
            .unwrap_or_default()
    );
    if let Some(fragment) = location.fragment() {
        rel.push('#');
        rel.push_str(fragment);
    }
    if rel == "?" || rel.starts_with("//") {
        return "/".to_string();
    }
    rel
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn location_same_origin_preserves_query() {
        let base = "http://localhost:8787/ssr?happy=true";
        assert_eq!(
            normalize_worker_location("/ssr/?happy=true", base),
            "/ssr/?happy=true"
        );
    }

    #[test]
    fn location_bare_question_mark_becomes_root() {
        assert_eq!(
            normalize_worker_location("?", "http://localhost/ssr?happy=true"),
            "/"
        );
    }

    #[test]
    fn request_url_collapses_absolute_path_slashes() {
        assert_eq!(
            normalize_worker_request_url("http://host//ssr?happy=true"),
            "http://host/ssr?happy=true"
        );
    }
}
