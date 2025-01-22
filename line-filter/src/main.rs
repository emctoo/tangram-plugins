use std::io::{self, BufRead};
use std::path::PathBuf;

use clap::Parser;
use tracing::error;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

use line_filter::conf::{Config, Dispatcher};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// 工作目录路径
    #[arg(long, default_value = ".", help = "config file directory, woring directory by default")]
    config_dir: PathBuf,

    /// 配置文件路径
    #[arg(short, long, default_value = "config.toml", help = "config file name")]
    config_file: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let stdin = io::stdin();
    let reader = stdin.lock();

    let args = Args::parse(); // 解析命令行参数
    std::fs::create_dir_all(&args.config_dir)?;

    // 将配置文件路径与目录路径合并
    let config_path = args.config_dir.join(&args.config_file);
    let config = Config::load(config_path.to_str().unwrap())?;
    let dispatcher = Dispatcher::new(args.config_dir.to_str().unwrap().to_string(), config).await?;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            break;
        }
        if let Err(e) = dispatcher.process_message(&line).await {
            error!("Error processing message: {}", e);
        }
    }

    Ok(())
}
