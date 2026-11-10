//! `flare-plugin pack`: Ed25519 signatures are deterministic and `write_package` fixes everything
//! else, so the same directory and key give the same file.
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::pkcs8::DecodePrivateKey;
use ed25519_dalek::{Signer, SigningKey};
use flare_plugin_protocol::package::{
    PACKAGE_EXTENSION, PackageContents, PackageSignature, PluginPackage, write_package,
};
use flare_plugin_protocol::signature::PublisherTrustStore;

use crate::source;

pub fn pack(dir: &Path, key: &Path, key_id: &str, out: Option<&Path>) -> Result<PathBuf> {
    let source = source::load(dir)?;
    let pem = fs::read_to_string(key).with_context(|| format!("reading {}", key.display()))?;
    let key = SigningKey::from_pkcs8_pem(&pem)
        .map_err(|_| anyhow::anyhow!("the key is not a PKCS#8 PEM Ed25519 private key"))?;
    let manifest_json = source.signed_bytes()?;
    let signature = PackageSignature {
        key_id: key_id.to_owned(),
        signature: STANDARD.encode(key.sign(&manifest_json).to_bytes()),
    };
    let package = write_package(&PackageContents {
        manifest_json: &manifest_json,
        signature: &signature,
        assets: &source.assets,
        readme: source.readme.as_deref(),
        changelog: source.changelog.as_deref(),
    })?;
    // The check the platform runs first, with this key trusted: what fails here fails there.
    let mut trust = PublisherTrustStore::default();
    trust.insert(
        key_id.to_owned(),
        source.manifest.publisher.publisher_id.clone(),
        key.verifying_key().to_bytes(),
    )?;
    PluginPackage::open(&package, &trust)?;
    let path = out.map(Path::to_path_buf).unwrap_or_else(|| {
        let manifest = &source.manifest;
        PathBuf::from(format!(
            "{}-{}.{PACKAGE_EXTENSION}",
            manifest.plugin_id, manifest.version
        ))
    });
    fs::write(&path, package).with_context(|| format!("writing {}", path.display()))?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaffold;
    use ed25519_dalek::pkcs8::EncodePrivateKey;
    use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;

    fn key(dir: &Path) -> (PathBuf, SigningKey) {
        let signing = SigningKey::from_bytes(&[7; 32]);
        let path = dir.join("publisher.pem");
        fs::write(
            &path,
            signing.to_pkcs8_pem(LineEnding::LF).unwrap().as_bytes(),
        )
        .unwrap();
        (path, signing)
    }

    fn trust(signing: &SigningKey) -> PublisherTrustStore {
        let mut trust = PublisherTrustStore::default();
        trust
            .insert(
                "k1".to_owned(),
                "acme".to_owned(),
                signing.verifying_key().to_bytes(),
            )
            .unwrap();
        trust
    }

    #[test]
    fn the_same_directory_and_key_give_the_same_package() {
        let parent = tempfile::tempdir().unwrap();
        let dir = scaffold::plugin(parent.path(), "acme.poll").unwrap();
        let (key_path, signing) = key(parent.path());
        let (first, second) = (parent.path().join("a.fpkg"), parent.path().join("b.fpkg"));
        pack(&dir, &key_path, "k1", Some(&first)).unwrap();
        pack(&dir, &key_path, "k1", Some(&second)).unwrap();
        let bytes = fs::read(&first).unwrap();
        assert_eq!(bytes, fs::read(&second).unwrap());
        let opened = PluginPackage::open(&bytes, &trust(&signing)).unwrap();
        assert_eq!(opened.manifest().manifest().plugin_id, "acme.poll");
        assert_eq!(
            opened.manifest_json(),
            fs::read(dir.join("manifest.json")).unwrap()
        );
        assert!(opened.readme().is_some());
    }

    #[test]
    fn the_asset_table_is_signed_from_the_directory() {
        let parent = tempfile::tempdir().unwrap();
        let dir = scaffold::plugin(parent.path(), "acme.poll").unwrap();
        fs::create_dir_all(dir.join("assets/screens")).unwrap();
        fs::write(dir.join("assets/screens/one.png"), b"one").unwrap();
        fs::write(dir.join("assets/.DS_Store"), b"noise").unwrap();
        let on_disk = fs::read(dir.join("manifest.json")).unwrap();
        let (key_path, signing) = key(parent.path());
        let out = parent.path().join("a.fpkg");
        pack(&dir, &key_path, "k1", Some(&out)).unwrap();
        let opened = PluginPackage::open(&fs::read(&out).unwrap(), &trust(&signing)).unwrap();
        assert_eq!(opened.assets().len(), 1);
        assert_eq!(opened.assets()["assets/screens/one.png"], b"one");
        assert_eq!(opened.manifest().manifest().assets.len(), 1);
        assert_eq!(fs::read(dir.join("manifest.json")).unwrap(), on_disk);
    }

    #[test]
    fn a_key_that_is_not_pem_is_refused() {
        let parent = tempfile::tempdir().unwrap();
        let dir = scaffold::plugin(parent.path(), "acme.poll").unwrap();
        let path = parent.path().join("publisher.pem");
        fs::write(&path, "not a key").unwrap();
        assert!(pack(&dir, &path, "k1", None).is_err());
    }
}
