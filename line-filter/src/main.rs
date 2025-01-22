use std::io::{self, BufRead};
// use std::path::PathBuf;

use clap::Parser;
use tracing::error;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

use line_filter::conf::{Config, Dispatcher};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
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
        .with_writer(std::io::stdout)
        .with_writer(move || {
            struct Writer(std::io::Stdout);
            impl std::io::Write for Writer {
                fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                    // 在每个输出前添加回车符
                    print!("\r");
                    self.0.write(buf)
                }
                fn flush(&mut self) -> io::Result<()> {
                    self.0.flush()
                }
            }
            Writer(std::io::stdout())
        })
        .init();

    let stdin = io::stdin();
    let reader = stdin.lock();

    let args = Args::parse(); // 解析命令行参数
    let config = Config::load(&args.config_file)?;
    let dispatcher = Dispatcher::new(args.config_file.to_string(), config).await?;

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
