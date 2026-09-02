// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Outbound `fetch` loopback planning: canonical origin binding and fail-closed URL rules
//! (NITRO-CELLD-LOOPBACK-DESIGN v0.2 §5.2).

use std::net::{IpAddr, Ipv6Addr};

use celld_logic::http::{authority_decision, canonical_scheme, AuthorityDecision};
use url::Url;

/// Immutable loopback configuration for one worker generation.
#[derive(Clone, Debug)]
pub struct LoopbackConfig {
    /// Full canonical origin URL used as the relative-URL base (`scheme://host:port/`).
    pub canonical_inbound_url: String,
    pub synthetic_host: String,
    pub scheme: String,
    pub port: u16,
    /// Platform / gateway / operator ports that must not be reached via reqwest.
    pub denied_ports: Vec<u16>,
    pub loopback_enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FetchPlan {
    Loopback { url: String, host: String },
    Egress,
    Reject(String),
}

const DEFAULT_DENIED_PORTS: &[u16] = &[8787, 8790, 8791, 8792, 8793, 8794, 8795, 8796, 8797, 8798, 8799];

pub fn denied_ports_from_env() -> Vec<u16> {
    std::env::var("CELLD_FETCH_DENIED_PORTS")
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|part| part.trim().parse().ok())
                .collect()
        })
        .filter(|ports: &Vec<u16>| !ports.is_empty())
        .unwrap_or_else(|| DEFAULT_DENIED_PORTS.to_vec())
}

pub fn loopback_flag_enabled() -> bool {
    crate::env_vars::flag("CELLD_FETCH_LOOPBACK", true).unwrap_or(true)
}

/// Build config from deploy `vars` (`PUBLIC_BASE_URL`) and process env.
pub fn config_from_vars(vars: &[(String, String)]) -> Option<LoopbackConfig> {
    let public_base = vars
        .iter()
        .find(|(name, _)| name == "PUBLIC_BASE_URL")
        .map(|(_, value)| value.as_str())
        .filter(|value| !value.is_empty());
    let Some(public_base) = public_base else {
        return None;
    };
    let parsed = Url::parse(public_base).ok()?;
    let scheme = canonical_scheme(parsed.scheme()).unwrap_or(parsed.scheme()).to_string();
    let host = parsed.host_str()?.to_string();
    if !authority_ok(&host, parsed.port()) {
        return None;
    }
    let port = parsed.port_or_known_default().unwrap_or(if scheme == "https" { 443 } else { 80 });
    let canonical_inbound_url = format!("{}://{}:{}/", scheme, host, port);
    let loopback_enabled = loopback_flag_enabled();
    Some(LoopbackConfig {
        canonical_inbound_url,
        synthetic_host: host,
        scheme,
        port,
        denied_ports: denied_ports_from_env(),
        loopback_enabled,
    })
}

fn authority_ok(host: &str, port: Option<u16>) -> bool {
    let authority = match port {
        Some(port) => format!("{}:{}", host, port),
        None => host.to_string(),
    };
    matches!(
        authority_decision(&authority),
        AuthorityDecision::Use(_) | AuthorityDecision::NeedsUrlParser(_)
    )
}

fn normalize_host(host: &str) -> Option<String> {
    let host = host.trim_end_matches('.');
    if host.is_empty() {
        return None;
    }
    let lower = host.to_ascii_lowercase();
    if lower == "localhost" || lower.ends_with(".localhost") {
        return Some(lower);
    }
    Some(lower)
}

fn origins_match(resolved: &Url, config: &LoopbackConfig) -> bool {
    let scheme = canonical_scheme(resolved.scheme()).unwrap_or(resolved.scheme());
    if scheme != config.scheme.as_str() {
        return false;
    }
    let host = match resolved.host_str().and_then(|h| normalize_host(h)) {
        Some(host) => host,
        None => return false,
    };
    let config_host = match normalize_host(&config.synthetic_host) {
        Some(host) => host,
        None => return false,
    };
    if host != config_host {
        return false;
    }
    let port = resolved.port_or_known_default().unwrap_or(if scheme == "https" { 443 } else { 80 });
    port == config.port
}

fn is_metadata_host(host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    host == "metadata.google.internal" || host.ends_with(".metadata.google.internal")
}

fn is_restricted_endpoint(host: &str, port: u16, denied_ports: &[u16]) -> bool {
    if !denied_ports.contains(&port) {
        return false;
    }
    if is_metadata_host(host) {
        return true;
    }
    if host == "localhost" || host.ends_with(".localhost") {
        return true;
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return is_restricted_ip(ip);
    }
  // Bracketed IPv6 in URL host_str is without brackets
    if let Ok(ip) = parse_ipv6_literal(host) {
        return is_restricted_ip(ip);
    }
    false
}

fn parse_ipv6_literal(host: &str) -> Result<IpAddr, ()> {
    if host.starts_with('[') && host.ends_with(']') {
        return host[1..host.len() - 1].parse().map_err(|_| ());
    }
    host.parse().map_err(|_| ())
}

const PLATFORM_LISTENER_MSG: &str = "fetch: disallowed request to a local platform listener";

/// Fail-closed: any host on a denied platform port, plus metadata / loopback / RFC1918 on those ports.
fn platform_listener_reject(host: &str, port: u16, denied_ports: &[u16]) -> Option<FetchPlan> {
    if denied_ports.contains(&port) {
        return Some(FetchPlan::Reject(PLATFORM_LISTENER_MSG.into()));
    }
    if is_metadata_host(host) {
        return Some(FetchPlan::Reject(PLATFORM_LISTENER_MSG.into()));
    }
    if is_restricted_endpoint(host, port, denied_ports) {
        return Some(FetchPlan::Reject(PLATFORM_LISTENER_MSG.into()));
    }
    None
}

fn is_restricted_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.octets()[0] == 169 && v4.octets()[1] == 254
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
                || v6 == Ipv6Addr::UNSPECIFIED
        }
    }
}

pub fn plan_fetch(url: &str, inbound_base: &str, config: Option<&LoopbackConfig>) -> FetchPlan {
    let inbound = inbound_base.trim();
    let base = if inbound.is_empty() {
        config.map(|c| c.canonical_inbound_url.as_str()).unwrap_or("")
    } else {
        inbound
    };
    if base.is_empty() {
        return classify_without_canonical(url);
    }
    let base_url = match Url::parse(base) {
        Ok(url) => url,
        Err(_) => return FetchPlan::Reject("fetch: invalid inbound base URL".into()),
    };
    let resolved = match Url::options().base_url(Some(&base_url)).parse(url) {
        Ok(url) => url,
        Err(_) => return FetchPlan::Reject("fetch: invalid URL".into()),
    };
    if resolved.scheme() == "ws" || resolved.scheme() == "wss" {
        return FetchPlan::Egress;
    }
    if resolved.scheme() != "http" && resolved.scheme() != "https" {
        return FetchPlan::Reject("fetch: unsupported URL scheme".into());
    }
    let host = resolved.host_str().unwrap_or("");
    let port = resolved
        .port_or_known_default()
        .unwrap_or(if resolved.scheme() == "https" { 443 } else { 80 });
    if let Some(config) = config {
        let canonical_match = origins_match(&resolved, config);
        if canonical_match {
            if config.loopback_enabled {
                let host_header = config.synthetic_host.clone();
                let mut loopback_url = resolved.clone();
                loopback_url.set_host(Some(&host_header)).ok();
                return FetchPlan::Loopback {
                    url: loopback_url.to_string(),
                    host: host_header,
                };
            }
            return FetchPlan::Reject(
                "fetch: same-origin fetch must use in-process loopback".into(),
            );
        }
        if let Some(plan) = platform_listener_reject(host, port, &config.denied_ports) {
            return plan;
        }
    } else if let Some(plan) = platform_listener_reject(host, port, DEFAULT_DENIED_PORTS) {
        return plan;
    }
    FetchPlan::Egress
}

fn classify_without_canonical(url: &str) -> FetchPlan {
    let parsed = match Url::parse(url) {
        Ok(url) => url,
        Err(_) => return FetchPlan::Reject("fetch: invalid URL".into()),
    };
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return FetchPlan::Reject("fetch: unsupported URL scheme".into());
    }
    let host = parsed.host_str().unwrap_or("");
    let port = parsed
        .port_or_known_default()
        .unwrap_or(if parsed.scheme() == "https" { 443 } else { 80 });
    if let Some(plan) = platform_listener_reject(host, port, DEFAULT_DENIED_PORTS) {
        return plan;
    }
    FetchPlan::Egress
}

pub fn plan_to_json(plan: &FetchPlan) -> String {
    match plan {
        FetchPlan::Loopback { url, host } => {
            serde_json::json!({ "action": "loopback", "url": url, "host": host }).to_string()
        }
        FetchPlan::Egress => serde_json::json!({ "action": "egress" }).to_string(),
        FetchPlan::Reject(message) => {
            serde_json::json!({ "action": "reject", "error": message }).to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> LoopbackConfig {
        LoopbackConfig {
            canonical_inbound_url: "http://support-flaremo.lvh.me:8787/".into(),
            synthetic_host: "support-flaremo.lvh.me".into(),
            scheme: "http".into(),
            port: 8787,
            denied_ports: DEFAULT_DENIED_PORTS.to_vec(),
            loopback_enabled: true,
        }
    }

    #[test]
    fn canonical_absolute_loopback() {
        let plan = plan_fetch(
            "http://support-flaremo.lvh.me:8787/api",
            "http://support-flaremo.lvh.me:8787/",
            Some(&cfg()),
        );
        assert_eq!(
            plan,
            FetchPlan::Loopback {
                url: "http://support-flaremo.lvh.me:8787/api".into(),
                host: "support-flaremo.lvh.me".into(),
            }
        );
    }

    #[test]
    fn relative_path_uses_canonical_base() {
        let plan = plan_fetch("/api", "http://support-flaremo.lvh.me:8787/", Some(&cfg()));
        assert!(matches!(plan, FetchPlan::Loopback { .. }));
    }

    #[test]
    fn gateway_loopback_host_wrong_host_rejects() {
        let plan = plan_fetch(
            "http://127.0.0.1:8787/",
            "http://support-flaremo.lvh.me:8787/",
            Some(&cfg()),
        );
        assert!(matches!(plan, FetchPlan::Reject(_)));
    }

    #[test]
    fn external_egress() {
        let plan = plan_fetch("https://example.com/", "http://support-flaremo.lvh.me:8787/", Some(&cfg()));
        assert_eq!(plan, FetchPlan::Egress);
    }

    #[test]
    fn other_project_host_on_gateway_port_rejects() {
        let plan = plan_fetch(
            "http://other-project.lvh.me:8787/",
            "http://support-flaremo.lvh.me:8787/",
            Some(&cfg()),
        );
        assert!(matches!(plan, FetchPlan::Reject(_)));
    }

    #[test]
    fn celld_operator_port_rejects() {
        let plan = plan_fetch(
            "http://support-flaremo.lvh.me:8792/api",
            "http://support-flaremo.lvh.me:8787/",
            Some(&cfg()),
        );
        assert!(matches!(plan, FetchPlan::Reject(_)));
    }

    #[test]
    fn ipv6_loopback_gateway_port_rejects() {
        let plan = plan_fetch(
            "http://[::1]:8787/",
            "http://support-flaremo.lvh.me:8787/",
            Some(&cfg()),
        );
        assert!(matches!(plan, FetchPlan::Reject(_)));
    }

    #[test]
    fn rfc1918_on_gateway_port_rejects() {
        let plan = plan_fetch(
            "http://10.0.0.1:8787/",
            "http://support-flaremo.lvh.me:8787/",
            Some(&cfg()),
        );
        assert!(matches!(plan, FetchPlan::Reject(_)));
    }

    #[test]
    fn metadata_host_rejects() {
        let plan = plan_fetch(
            "http://metadata.google.internal/computeMetadata/v1/",
            "http://support-flaremo.lvh.me:8787/",
            Some(&cfg()),
        );
        assert!(matches!(plan, FetchPlan::Reject(_)));
    }

    #[test]
    fn gateway_port_without_config_rejects_hostname() {
        let plan = plan_fetch("http://victim.example:8787/", "", None);
        assert!(matches!(plan, FetchPlan::Reject(_)));
    }

    #[test]
    fn loopback_disabled_same_origin_rejects_not_egress() {
        let mut disabled = cfg();
        disabled.loopback_enabled = false;
        let plan = plan_fetch(
            "http://support-flaremo.lvh.me:8787/api",
            "http://support-flaremo.lvh.me:8787/",
            Some(&disabled),
        );
        assert!(matches!(
            plan,
            FetchPlan::Reject(message) if message.contains("in-process loopback")
        ));
    }

    #[test]
    fn same_origin_host_case_insensitive_loopback() {
        let plan = plan_fetch(
            "http://SUPPORT-FLAREMO.lvh.me:8787/ping",
            "http://support-flaremo.lvh.me:8787/",
            Some(&cfg()),
        );
        assert!(matches!(plan, FetchPlan::Loopback { .. }));
    }

    #[test]
    fn plan_to_json_loopback_for_harness() {
        let plan = FetchPlan::Loopback {
            url: "http://support-flaremo.lvh.me:8787/api".into(),
            host: "support-flaremo.lvh.me".into(),
        };
        let json: serde_json::Value = serde_json::from_str(&plan_to_json(&plan)).unwrap();
        assert_eq!(json["action"], "loopback");
        assert_eq!(json["url"], "http://support-flaremo.lvh.me:8787/api");
        assert_eq!(json["host"], "support-flaremo.lvh.me");
    }

    #[test]
    fn config_from_vars_reads_public_base_url() {
        let vars = vec![
            ("PUBLIC_BASE_URL".into(), "http://app.example:8787".into()),
            ("OTHER".into(), "x".into()),
        ];
        let config = config_from_vars(&vars).expect("PUBLIC_BASE_URL");
        assert_eq!(config.synthetic_host, "app.example");
        assert_eq!(config.port, 8787);
        assert_eq!(config.canonical_inbound_url, "http://app.example:8787/");
    }
}
