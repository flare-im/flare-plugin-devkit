//! The `flare-plugin` command. Each subcommand is one module.
mod check;
mod dev;
mod pack;
mod publish;
mod scaffold;
mod source;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(
    name = "flare-plugin",
    version,
    about = "Build and publish Flare plugins"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a plugin or a hook service in a new directory named after it
    New {
        #[arg(value_enum)]
        kind: Kind,
        /// A plugin id such as `acme.poll`, or a hook crate name such as `spam-filter`
        name: String,
    },
    /// Validate a plugin directory and report its derived kind
    Check { dir: PathBuf },
    /// Sign a plugin directory and write its reproducible .fpkg package
    Pack {
        dir: PathBuf,
        /// PKCS#8 PEM Ed25519 private key of the publisher
        #[arg(long)]
        key: PathBuf,
        /// The id the platform trusts this key under
        #[arg(long)]
        key_id: String,
        /// Defaults to `<pluginId>-<version>.fpkg` in the current directory
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Upload a package to a platform, which registers the version unlisted
    Publish {
        file: PathBuf,
        /// Origin of the console, such as `https://console.example.com`
        #[arg(long)]
        url: String,
        /// Personal access token with `plugins:catalog:write`
        #[arg(long, env = "FLARE_PLUGIN_TOKEN", hide_env_values = true)]
        token: String,
        /// Audit reason; defaults to naming the file
        #[arg(long)]
        reason: Option<String>,
    },
    /// Run a local process as this plugin in an application flagged for development, and
    /// re-register it whenever `manifest.json` changes. Runs until interrupted
    Dev {
        dir: PathBuf,
        /// The application, flagged for development by platform staff
        #[arg(long)]
        app: String,
        /// Origin of the Core admin gateway, such as `http://127.0.0.1:50051`
        #[arg(long)]
        url: String,
        /// Staff token of the application with `core:plugins:write`
        #[arg(long, env = "FLARE_PLUGIN_TOKEN", hide_env_values = true)]
        token: String,
        /// Where Core reaches the local process
        #[arg(
            long,
            env = "FLARE_PLUGIN_GRPC_AUTHORITY",
            default_value = "http://127.0.0.1:7900"
        )]
        process: String,
        /// Must start with `dev-`; defaults to `dev-<pluginId>`. The process serves under this id
        #[arg(long)]
        deployment_id: Option<String>,
        /// A JSON file with the plugin's configuration; defaults to `{}`
        #[arg(long)]
        config: Option<PathBuf>,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum Kind {
    Plugin,
    Hook,
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::New { kind, name } => {
            let parent = std::env::current_dir()?;
            let created = match kind {
                Kind::Plugin => scaffold::plugin(&parent, &name)?,
                Kind::Hook => scaffold::hook(&parent, &name)?,
            };
            println!("created {}", created.display());
        }
        Command::Check { dir } => {
            let source = source::load(&dir)?;
            print!("{}", check::report(&source));
        }
        Command::Pack {
            dir,
            key,
            key_id,
            out,
        } => {
            let written = pack::pack(&dir, &key, &key_id, out.as_deref())?;
            println!("wrote {}", written.display());
        }
        Command::Publish {
            file,
            url,
            token,
            reason,
        } => println!(
            "{}",
            publish::publish(&file, &url, &token, reason.as_deref())?
        ),
        Command::Dev {
            dir,
            app,
            url,
            token,
            process,
            deployment_id,
            config,
        } => dev::run(&dev::Options {
            dir,
            app,
            url,
            token,
            process,
            deployment_id,
            config,
        })?,
    }
    Ok(())
}
