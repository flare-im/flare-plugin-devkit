//! `flare-plugin new`: a new directory, never over an existing one.
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use flare_plugin_protocol::PluginManifest;
use flare_plugin_protocol::package::{CHANGELOG_ENTRY, MANIFEST_ENTRY, README_ENTRY};

const PLUGIN_MANIFEST: &str = include_str!("../../template/platform-plugin.template.json");
const PLUGIN_README: &str = include_str!("../templates/plugin/README.md.tmpl");
const PLUGIN_CHANGELOG: &str = include_str!("../templates/plugin/CHANGELOG.md.tmpl");
const HOOK_CARGO: &str = include_str!("../templates/hook/Cargo.toml.tmpl");
const HOOK_MAIN: &str = include_str!("../templates/hook/main.rs.tmpl");
const HOOK_README: &str = include_str!("../templates/hook/README.md.tmpl");
const HOOK_CONFIG: &str = include_str!("../templates/hook/hooks.toml.tmpl");
const HOOK_GITIGNORE: &str = include_str!("../templates/hook/gitignore.tmpl");
const NAME_SLOT: &str = "{{name}}";

/// The manifest template under a new identity. The first segment of the id names the publisher.
fn manifest_for(plugin_id: &str) -> String {
    let publisher = plugin_id.split('.').next().unwrap_or(plugin_id);
    PLUGIN_MANIFEST
        .lines()
        // Relative to the template directory, so it would point nowhere from the new one.
        .filter(|line| !line.trim_start().starts_with("\"$schema\""))
        .collect::<Vec<_>>()
        .join("\n")
        .replace("example.notice", plugin_id)
        .replace(
            "\"publisherId\": \"example\"",
            &format!("\"publisherId\": \"{publisher}\""),
        )
        .replace("Example Notice Plugin", plugin_id)
        .replace("Example Publisher", publisher)
        + "\n"
}

pub fn plugin(parent: &Path, plugin_id: &str) -> Result<PathBuf> {
    let manifest = manifest_for(plugin_id);
    PluginManifest::from_json(manifest.as_bytes()).with_context(|| {
        format!("{plugin_id} is not a plugin id: expected a form like acme.poll")
    })?;
    let readme = PLUGIN_README.replace(NAME_SLOT, plugin_id);
    write_new(
        &parent.join(plugin_id),
        &[
            (MANIFEST_ENTRY, manifest.as_str()),
            (README_ENTRY, readme.as_str()),
            (CHANGELOG_ENTRY, PLUGIN_CHANGELOG),
        ],
    )
}

pub fn hook(parent: &Path, name: &str) -> Result<PathBuf> {
    let valid = name.starts_with(|c: char| c.is_ascii_lowercase())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if !valid {
        bail!("{name} is not a hook name: expected a form like spam-filter");
    }
    let fill = |template: &str| template.replace(NAME_SLOT, name);
    write_new(
        &parent.join(name),
        &[
            ("Cargo.toml", fill(HOOK_CARGO).as_str()),
            ("src/main.rs", fill(HOOK_MAIN).as_str()),
            ("README.md", fill(HOOK_README).as_str()),
            ("hooks.toml", fill(HOOK_CONFIG).as_str()),
            (".gitignore", HOOK_GITIGNORE),
        ],
    )
}

fn write_new(dir: &Path, files: &[(&str, &str)]) -> Result<PathBuf> {
    if dir.exists() {
        bail!("{} already exists", dir.display());
    }
    for (name, content) in files {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, content).with_context(|| format!("writing {}", path.display()))?;
    }
    Ok(dir.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plugin_takes_its_identity_from_the_name() {
        let parent = tempfile::tempdir().unwrap();
        let dir = plugin(parent.path(), "acme.poll").unwrap();
        let manifest =
            PluginManifest::from_json(&fs::read(dir.join(MANIFEST_ENTRY)).unwrap()).unwrap();
        assert_eq!(manifest.plugin_id, "acme.poll");
        assert_eq!(manifest.publisher.publisher_id, "acme");
        assert_eq!(manifest.capabilities[0].id, "acme.poll");
        assert!(manifest.schema.is_none());
    }

    #[test]
    fn a_name_that_cannot_be_an_id_is_refused() {
        let parent = tempfile::tempdir().unwrap();
        assert!(plugin(parent.path(), "Acme Poll").is_err());
        assert!(hook(parent.path(), "../spam").is_err());
    }

    #[test]
    fn a_hook_is_filled_in_and_handles_pre_send() {
        let parent = tempfile::tempdir().unwrap();
        let dir = hook(parent.path(), "spam-filter").unwrap();
        let main = fs::read_to_string(dir.join("src/main.rs")).unwrap();
        assert!(main.contains("flare.hook.v1.pre_send"));
        for file in ["Cargo.toml", "src/main.rs", "README.md", "hooks.toml"] {
            let text = fs::read_to_string(dir.join(file)).unwrap();
            assert!(!text.contains(NAME_SLOT), "{file}");
        }
    }

    #[test]
    fn an_existing_directory_is_left_alone() {
        let parent = tempfile::tempdir().unwrap();
        fs::create_dir(parent.path().join("spam-filter")).unwrap();
        assert!(hook(parent.path(), "spam-filter").is_err());
    }
}
