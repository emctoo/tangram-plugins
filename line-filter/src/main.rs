use std::io::{self, BufRead};
use tracing::error;
use clap::Parser;

use line_filter::conf::{Config, Dispatcher};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// 配置文件路径
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let reader = stdin.lock();

    let args = Args::parse();    // 解析命令行参数
    let config = Config::load(&args.config)?;     // 加载配置  
    let dispatcher = Dispatcher::new(config).await?;

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
