//! Automatic workload exchange. Reopen projected files for every request so
//! kubelet rotations are observed; neither bearer nor registration tokens are cached.
use crate::{HostError, RegistrationTokenFuture, RegistrationTokenProvider};
use flare_plugin_protocol::workload::{
    RegistrationCredential, RegistrationExchange, RegistrationRequest,
};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

pub struct WorkloadTokenProvider {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    token_file: PathBuf,
    deployment_id: String,
}
impl WorkloadTokenProvider {
    pub fn new(
        endpoint: &str,
        token_file: PathBuf,
        deployment_id: String,
        ca_pem: Option<&[u8]>,
    ) -> Result<Self, HostError> {
        let endpoint =
            reqwest::Url::parse(endpoint).map_err(|_| HostError::Invalid("invalid broker URL"))?;
        let loopback = endpoint
            .host_str()
            .and_then(|h| h.trim_matches(['[', ']']).parse::<std::net::IpAddr>().ok())
            .is_some_and(|ip| ip.is_loopback());
        if !(endpoint.scheme() == "https" || endpoint.scheme() == "http" && loopback)
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || endpoint.path() != flare_plugin_protocol::workload::BROKER_PATH
            || !token_file.is_absolute()
            || deployment_id.is_empty()
            || deployment_id.len() > 128
            || !deployment_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        {
            return Err(HostError::Invalid("invalid workload broker configuration"));
        }
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(4));
        if let Some(pem) = ca_pem {
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(pem)
                    .map_err(|_| HostError::Invalid("invalid broker CA"))?,
            );
        }
        Ok(Self {
            client: builder
                .build()
                .map_err(|_| HostError::Invalid("invalid broker client"))?,
            endpoint,
            token_file,
            deployment_id,
        })
    }
    pub fn from_env() -> Result<Self, HostError> {
        let env = |name| {
            std::env::var(name)
                .map_err(|_| HostError::Invalid("workload broker configuration missing"))
        };
        let ca = std::env::var_os("FLARE_PLUGIN_BROKER_CA_FILE")
            .map(|p| std::fs::read(p).map_err(|_| HostError::Invalid("broker CA unavailable")))
            .transpose()?;
        Self::new(
            &env("FLARE_PLUGIN_BROKER_URL")?,
            PathBuf::from(env("FLARE_PLUGIN_WORKLOAD_TOKEN_FILE")?),
            env("FLARE_PLUGIN_DEPLOYMENT_ID")?,
            ca.as_deref(),
        )
    }
}
async fn read_token(path: &Path) -> Result<String, HostError> {
    use tokio::io::AsyncReadExt;
    let mut token = String::new();
    tokio::fs::File::open(path)
        .await
        .map_err(|_| HostError::Sign("projected workload token unavailable".into()))?
        .take(16 * 1024 + 1)
        .read_to_string(&mut token)
        .await
        .map_err(|_| HostError::Sign("projected workload token unavailable".into()))?;
    let token = token.trim();
    if token.is_empty() || token.len() > 16384 || !token.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(HostError::Sign("invalid projected workload token".into()));
    }
    Ok(token.to_owned())
}
impl RegistrationTokenProvider for WorkloadTokenProvider {
    fn token<'a>(&'a self, request: &'a RegistrationRequest) -> RegistrationTokenFuture<'a> {
        Box::pin(async move {
            request
                .validate()
                .map_err(|_| HostError::Invalid("invalid registration request"))?;
            let token = read_token(&self.token_file).await?;
            let mut response = self
                .client
                .post(self.endpoint.clone())
                .bearer_auth(token)
                .json(&RegistrationExchange {
                    deployment_id: self.deployment_id.clone(),
                    request: request.clone(),
                })
                .send()
                .await
                .map_err(|_| HostError::Sign("workload broker unavailable".into()))?;
            if !response.status().is_success() {
                return Err(HostError::Sign(
                    "workload broker denied registration".into(),
                ));
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| HostError::Sign("invalid broker response".into()))?
            {
                if bytes.len() + chunk.len() > 16384 {
                    return Err(HostError::Sign("broker response exceeds limit".into()));
                }
                bytes.extend_from_slice(&chunk);
            }
            let credential: RegistrationCredential = serde_json::from_slice(&bytes)
                .map_err(|_| HostError::Sign("invalid broker response".into()))?;
            if credential.token.is_empty() || credential.token.len() > 8192 {
                return Err(HostError::Sign("invalid broker credential".into()));
            }
            Ok(credential.token)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn endpoint_configuration_rejects_insecure_remote_redirect_credentials_and_queries() {
        for prefix in [
            "http://remote",
            "http://localhost",
            "ftp://127.0.0.1",
            "https://user:password@broker",
        ] {
            assert!(
                WorkloadTokenProvider::new(
                    &format!("{prefix}{}", flare_plugin_protocol::workload::BROKER_PATH),
                    "/projected/token".into(),
                    "deployment-1".into(),
                    None
                )
                .is_err()
            );
        }
        assert!(
            WorkloadTokenProvider::new(
                &format!(
                    "https://broker{}",
                    flare_plugin_protocol::workload::BROKER_PATH
                ),
                "/projected/token".into(),
                "deployment-1".into(),
                None
            )
            .is_ok()
        );
    }
}
