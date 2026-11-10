//! `flare-plugin publish`: uploads a package over the admin API.
use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use flare_plugin_protocol::package::MAX_PACKAGE_BYTES;
use reqwest::blocking::Client;
use reqwest::header::CONTENT_TYPE;
use serde_json::Value;

const PACKAGES_PATH: &str = "/admin/v1/plugins/packages";
/// The route's own deadline is 30 s; this only bounds a stalled connection.
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(60);

/// Returns the registered identity for the operator to read.
pub fn publish(file: &Path, url: &str, token: &str, reason: Option<&str>) -> Result<String> {
    let bytes = fs::read(file).with_context(|| format!("reading {}", file.display()))?;
    if bytes.len() as u64 > MAX_PACKAGE_BYTES {
        bail!("{} exceeds {MAX_PACKAGE_BYTES} bytes", file.display());
    }
    let name = file.file_name().unwrap_or_default().to_string_lossy();
    let reason = reason.map_or_else(|| format!("publish {name}"), str::to_owned);
    let endpoint = format!("{}{PACKAGES_PATH}", url.trim_end_matches('/'));
    let response = Client::builder()
        .timeout(UPLOAD_TIMEOUT)
        .build()?
        .post(&endpoint)
        .bearer_auth(token)
        .header(CONTENT_TYPE, "application/octet-stream")
        .header("idempotency-key", uuid::Uuid::new_v4().to_string())
        .header("x-audit-reason", percent_encoded(&reason))
        .body(bytes)
        .send()
        .with_context(|| format!("uploading to {endpoint}"))?;
    let status = response.status();
    let body = response.text()?;
    if !status.is_success() {
        bail!("{endpoint} answered {status}: {body}");
    }
    let reply: Value = serde_json::from_str(&body).context("reading the platform's reply")?;
    Ok(format!(
        "registered {} {} ({})\nIt stays unlisted until you publish the version in the console.",
        reply["data"]["plugin_id"].as_str().unwrap_or("?"),
        reply["data"]["plugin_version"].as_str().unwrap_or("?"),
        reply["data"]["manifest_digest"].as_str().unwrap_or("?"),
    ))
}

/// The `x-audit-reason` wire form: UTF-8 with everything but unreserved bytes escaped.
pub(crate) fn percent_encoded(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                char::from(byte).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reason_is_percent_encoded_as_utf8() {
        assert_eq!(
            percent_encoded("publish a-1.0.fpkg"),
            "publish%20a-1.0.fpkg"
        );
        assert_eq!(percent_encoded("上架"), "%E4%B8%8A%E6%9E%B6");
    }
}
