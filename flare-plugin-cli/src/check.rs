//! `flare-plugin check`: what is wrong is an error from [`crate::source::load`]; this reports what
//! the platform would accept but a release should not ship with.
use std::fmt::Write;

use flare_plugin_protocol::manifest::{PluginKind, PluginRuntime};

use crate::source::PluginSource;

const PLACEHOLDER_DIGEST: &str =
    "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const PLACEHOLDER_SIGNATURE_PREFIX: &str = "REPLACE_";

pub fn warnings(source: &PluginSource) -> Vec<String> {
    let manifest = &source.manifest;
    let mut found = Vec::new();
    match &manifest.artifact {
        Some(artifact)
            if artifact.digest == PLACEHOLDER_DIGEST
                || artifact
                    .signature_ref
                    .starts_with(PLACEHOLDER_SIGNATURE_PREFIX) =>
        {
            found.push("artifact still holds the template placeholders".to_owned());
        }
        None if !matches!(manifest.runtime, PluginRuntime::Client) => {
            found.push(
                "no artifact: a shared deployment cannot be bound to this version".to_owned(),
            );
        }
        _ => {}
    }
    if manifest.host_version_range.is_none() {
        found.push("no hostVersionRange: every host counts as compatible".to_owned());
    }
    for contribution in &manifest.client_extensions.contributions {
        if contribution.preview.is_none() {
            found.push(format!(
                "contribution {} has no preview: the market cannot render a sample",
                contribution.id
            ));
        }
    }
    if !source.asset_table_is_current() {
        found.push(
            "assets in the manifest differ from assets/: pack will sign the table of assets/"
                .to_owned(),
        );
    }
    if source.readme.is_none() {
        found.push("no README.md".to_owned());
    }
    if source.changelog.is_none() {
        found.push("no CHANGELOG.md".to_owned());
    }
    found
}

pub fn report(source: &PluginSource) -> String {
    let manifest = &source.manifest;
    let kind = match manifest.kind() {
        PluginKind::Im => "im",
        PluginKind::Business => "business",
    };
    let mut text = format!(
        "{} {}\nkind: {kind}\nassets: {}\n",
        manifest.plugin_id,
        manifest.version,
        source.assets.len()
    );
    for warning in warnings(source) {
        // Writing to a String cannot fail.
        let _ = writeln!(text, "warning: {warning}");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{scaffold, source};

    #[test]
    fn a_fresh_scaffold_is_valid_and_says_what_is_left_to_fill_in() {
        let parent = tempfile::tempdir().unwrap();
        let dir = scaffold::plugin(parent.path(), "acme.poll").unwrap();
        let text = report(&source::load(&dir).unwrap());
        assert!(
            text.starts_with("acme.poll 1.0.0\nkind: business\n"),
            "{text}"
        );
        assert!(text.contains("warning: artifact still holds"), "{text}");
        assert!(!text.contains("no README.md"), "{text}");
    }

    #[test]
    fn a_stale_asset_table_is_reported() {
        let parent = tempfile::tempdir().unwrap();
        let dir = scaffold::plugin(parent.path(), "acme.poll").unwrap();
        std::fs::create_dir(dir.join("assets")).unwrap();
        std::fs::write(dir.join("assets/icon.png"), b"icon").unwrap();
        let found = warnings(&source::load(&dir).unwrap());
        assert!(
            found.iter().any(|warning| warning.contains("assets/")),
            "{found:?}"
        );
    }
}
