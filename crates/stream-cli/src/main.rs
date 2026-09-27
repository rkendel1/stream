use clap::Parser;
use stream_cli::{run, Cli};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let output = run(cli, ".").await?;
    if !output.is_empty() {
        println!("{output}");
    }
    Ok(())
}
