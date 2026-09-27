use clap::Parser;
use stream_cli::{run, Cli};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let output = run(cli, stream_appport::discover_root()).await?;
    if !output.is_empty() {
        println!("{output}");
    }
    Ok(())
}
