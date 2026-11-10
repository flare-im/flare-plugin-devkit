//! `flare-plugin dev`: keeps a local process registered in an application flagged for development.
//! It polls `manifest.json`, because editors replace files in ways a watcher misses, and one small
//! file costs nothing to read. Core does the rest: it applies the release to the process, and drops
//! the plugin from the application when the process stops answering.
use std::fs;
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use flare_plugin_protocol::package::MANIFEST_ENTRY;
use reqwest::blocking::Client;
use reqwest::header::CONTENT_TYPE;
use serde_json::{Map, Value, json};

use crate::publish::percent_encoded;
use crate::source;

const DEV_PATH: &str = "/api/v1/admin/plugin-registry/dev";
const AUDIT_REASON: &str = "flare-plugin dev";
const POLL: Duration = Duration::from_secs(1);
const RETRY: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct Options {
    pub dir: PathBuf,
    pub app: String,
    pub url: String,
    pub token: String,
    pub process: String,
    pub deployment_id: Option<String>,
    pub config: Option<PathBuf>,
}

enum Failure {
    /// The same input fails the same way: wait for an edit.
    Refused(anyhow::Error),
    /// The network or Core: try again.
    Transient(anyhow::Error),
}

/// What has been tried, by the bytes of the inputs.
#[derive(Default)]
struct Watch {
    registered: Option<Vec<u8>>,
    refused: Option<Vec<u8>>,
    retry_at: Option<Instant>,
}

impl Watch {
    fn due(&self, seen: &[u8], now: Instant) -> bool {
        self.registered.as_deref() != Some(seen)
            && self.refused.as_deref() != Some(seen)
            && self.retry_at.is_none_or(|at| now >= at)
    }

    fn succeeded(&mut self, seen: Vec<u8>) {
        *self = Self {
            registered: Some(seen),
            ..Self::default()
        };
    }

    fn was_refused(&mut self, seen: Vec<u8>) {
        self.refused = Some(seen);
        self.retry_at = None;
    }

    fn retry_later(&mut self, now: Instant) {
        self.retry_at = Some(now + RETRY);
    }
}

/// Runs until the command is interrupted. A refusal of the first registration ends it: a wrong
/// token or an application that is not flagged will not fix itself.
pub fn run(options: &Options) -> Result<()> {
    let client = Client::builder().timeout(REQUEST_TIMEOUT).build()?;
    let mut watch = Watch::default();
    let mut started = false;
    loop {
        // An editor can leave the file missing for a moment. Only the start needs it to be there.
        match inputs(options) {
            Ok(seen) if watch.due(&seen, Instant::now()) => match register(&client, options) {
                Ok(line) => {
                    println!("{line}");
                    watch.succeeded(seen);
                    started = true;
                }
                Err(Failure::Refused(error)) if !started => return Err(error),
                Err(Failure::Refused(error)) => {
                    eprintln!("{error:#}\nThe last registration stays; waiting for an edit.");
                    watch.was_refused(seen);
                }
                Err(Failure::Transient(error)) => {
                    eprintln!("{error:#}\nRetrying.");
                    watch.retry_later(Instant::now());
                }
            },
            Ok(_) => {}
            Err(error) if !started => return Err(error),
            Err(_) => {}
        }
        sleep(POLL);
    }
}

fn inputs(options: &Options) -> Result<Vec<u8>> {
    let path = options.dir.join(MANIFEST_ENTRY);
    let mut seen = fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    if let Some(config) = &options.config {
        seen.extend(fs::read(config).with_context(|| format!("reading {}", config.display()))?);
    }
    Ok(seen)
}

fn configuration(path: Option<&Path>) -> Result<Value> {
    let Some(path) = path else {
        return Ok(Value::Object(Map::new()));
    };
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let value: Value = serde_json::from_slice(&bytes)
        .with_context(|| format!("{} is not JSON", path.display()))?;
    if !value.is_object() {
        bail!("{} must hold a JSON object", path.display());
    }
    Ok(value)
}

/// `manifest.json` goes as it is on disk: Core must compute the digest the local process computes,
/// so the asset table is not filled in as `pack` does.
fn request_body(options: &Options) -> Result<Value> {
    let source = source::load(&options.dir)?;
    let deployment_id = options
        .deployment_id
        .clone()
        .unwrap_or_else(|| format!("dev-{}", source.manifest.plugin_id));
    Ok(json!({
        "app_id": options.app,
        "manifest_json": String::from_utf8(source.manifest_json)
            .context("manifest.json is not UTF-8")?,
        "deployment_id": deployment_id,
        "grpc_authority": options.process,
        "configuration": configuration(options.config.as_deref())?,
    }))
}

fn register(client: &Client, options: &Options) -> Result<String, Failure> {
    let body = request_body(options)
        .and_then(|body| Ok(serde_json::to_vec(&body)?))
        .map_err(Failure::Refused)?;
    let endpoint = format!("{}{DEV_PATH}", options.url.trim_end_matches('/'));
    let response = client
        .post(&endpoint)
        .bearer_auth(&options.token)
        .header("idempotency-key", uuid::Uuid::new_v4().to_string())
        .header("x-audit-reason", percent_encoded(AUDIT_REASON))
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .map_err(|error| Failure::Transient(anyhow!("reaching {endpoint}: {error}")))?;
    let status = response.status();
    let text = response
        .text()
        .map_err(|error| Failure::Transient(anyhow!("reading the answer: {error}")))?;
    if status.is_client_error() {
        return Err(Failure::Refused(anyhow!(
            "{endpoint} answered {status}: {text}"
        )));
    }
    if !status.is_success() {
        return Err(Failure::Transient(anyhow!(
            "{endpoint} answered {status}: {text}"
        )));
    }
    let reply: Value = serde_json::from_str(&text)
        .map_err(|error| Failure::Transient(anyhow!("reading the answer: {error}")))?;
    Ok(summary(&reply["data"], &options.app, &options.process))
}

fn summary(data: &Value, app: &str, process: &str) -> String {
    let text = |key: &str| data[key].as_str().unwrap_or("?").to_owned();
    let verb = if data["changed"].as_bool().unwrap_or(true) {
        "registered"
    } else {
        "unchanged:"
    };
    format!(
        "{verb} {} {} ({}) in application {app}, deployment {} at {process}",
        text("pluginId"),
        text("pluginVersion"),
        text("manifestDigest"),
        text("deploymentId"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_edit_is_tried_again_after_a_refusal() {
        let now = Instant::now();
        let mut watch = Watch::default();
        assert!(watch.due(b"a", now));
        watch.succeeded(b"a".to_vec());
        assert!(!watch.due(b"a", now));
        assert!(watch.due(b"b", now));
        watch.was_refused(b"b".to_vec());
        assert!(!watch.due(b"b", now));
        assert!(watch.due(b"c", now));
        // Back to what is registered: nothing to do.
        assert!(!watch.due(b"a", now));
    }

    #[test]
    fn a_failed_call_is_retried_after_a_pause() {
        let now = Instant::now();
        let mut watch = Watch::default();
        watch.retry_later(now);
        assert!(!watch.due(b"a", now));
        assert!(watch.due(b"a", now + RETRY));
        watch.succeeded(b"a".to_vec());
        assert!(watch.retry_at.is_none());
    }

    #[test]
    fn a_configuration_file_holds_an_object() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        assert_eq!(configuration(None).unwrap(), json!({}));
        fs::write(&path, "[]").unwrap();
        assert!(configuration(Some(&path)).is_err());
        fs::write(&path, r#"{"a":1}"#).unwrap();
        assert_eq!(configuration(Some(&path)).unwrap(), json!({"a":1}));
    }

    #[test]
    fn the_summary_says_whether_anything_changed() {
        let data = json!({
            "pluginId": "acme.poll",
            "pluginVersion": "1.0.0",
            "manifestDigest": "sha256:x",
            "deploymentId": "dev-acme.poll",
            "changed": false,
        });
        assert_eq!(
            summary(&data, "201", "http://127.0.0.1:7900"),
            "unchanged: acme.poll 1.0.0 (sha256:x) in application 201, \
             deployment dev-acme.poll at http://127.0.0.1:7900"
        );
    }
}
