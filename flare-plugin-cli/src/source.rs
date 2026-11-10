//! A plugin directory on disk: `manifest.json`, `assets/`, `README.md`, `CHANGELOG.md`.
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use flare_plugin_protocol::PluginManifest;
use flare_plugin_protocol::manifest::{PluginAsset, is_asset_path};
use flare_plugin_protocol::package::{CHANGELOG_ENTRY, MANIFEST_ENTRY, README_ENTRY};
use flare_plugin_protocol::runtime::digest_bytes;
use serde_json::Value;

pub struct PluginSource {
    pub manifest: PluginManifest,
    /// The bytes on disk.
    pub manifest_json: Vec<u8>,
    /// By package path.
    pub assets: BTreeMap<String, Vec<u8>>,
    pub readme: Option<Vec<u8>>,
    pub changelog: Option<Vec<u8>>,
}

impl PluginSource {
    /// What `assets/` holds, as the manifest lists it.
    fn asset_table(&self) -> Vec<PluginAsset> {
        self.assets
            .iter()
            .map(|(path, bytes)| PluginAsset {
                path: path.clone(),
                digest: digest_bytes(bytes),
                size: bytes.len() as u64,
            })
            .collect()
    }

    pub fn asset_table_is_current(&self) -> bool {
        let mut declared = self.manifest.assets.clone();
        declared.sort_by(|a, b| a.path.cmp(&b.path));
        declared == self.asset_table()
    }

    /// The bytes to sign: the file as it is when its asset table is right, otherwise the same JSON
    /// with the table `assets/` calls for. The file on disk is left alone.
    pub fn signed_bytes(&self) -> Result<Vec<u8>> {
        if self.asset_table_is_current() {
            return Ok(self.manifest_json.clone());
        }
        let mut manifest: Value = serde_json::from_slice(&self.manifest_json)?;
        manifest["assets"] = serde_json::to_value(self.asset_table())?;
        Ok(serde_json::to_vec_pretty(&manifest)?)
    }
}

pub fn load(dir: &Path) -> Result<PluginSource> {
    let path = dir.join(MANIFEST_ENTRY);
    let manifest_json = fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
    let manifest = PluginManifest::from_json(&manifest_json)
        .with_context(|| format!("{} is not a valid manifest", path.display()))?;
    let mut assets = BTreeMap::new();
    let root = dir.join("assets");
    if root.is_dir() {
        collect_assets(&root, "assets", &mut assets)?;
    }
    Ok(PluginSource {
        manifest,
        manifest_json,
        assets,
        readme: optional(&dir.join(README_ENTRY))?,
        changelog: optional(&dir.join(CHANGELOG_ENTRY))?,
    })
}

fn optional(path: &Path) -> Result<Option<Vec<u8>>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(Some(bytes))
}

/// Hidden files stay out, so `.DS_Store` never changes a package.
fn collect_assets(dir: &Path, prefix: &str, assets: &mut BTreeMap<String, Vec<u8>>) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            bail!("{} has a file name that is not UTF-8", dir.display());
        };
        if name.starts_with('.') {
            continue;
        }
        let path = format!("{prefix}/{name}");
        let kind = entry.file_type()?;
        if kind.is_dir() {
            collect_assets(&entry.path(), &path, assets)?;
        } else if kind.is_file() {
            if !is_asset_path(&path) {
                bail!("{path}: use letters, digits, `.`, `-` and `_`, within 256 bytes");
            }
            assets.insert(path, fs::read(entry.path())?);
        } else {
            bail!("{path}: assets are plain files");
        }
    }
    Ok(())
}
