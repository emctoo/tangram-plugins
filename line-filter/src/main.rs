use std::io::{self, BufRead};

use clap::Parser;
use redis::{AsyncCommands, RedisResult};
use tracing::error;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

use line_filter::conf::{Config, Dispatcher};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "line-filter", help = "app name")]
    line_speed: bool,

    #[arg(long, default_value = "redis://127.0.0.1:6379", help = "redis url, line speed mode only")]
    redis_url: String,

    #[arg(long, default_value = "line-speed", help = "redis topic, line speed mode only")]
    redis_topic: String,

    #[arg(short, long, default_value = "config.toml", help = "config file name")]
    config_file: String,

    #[arg(short, long, default_value = "line-filter.log", help = "log file path")]
    log_file: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if args.line_speed {
        return line_speed(&args.redis_url, &args.redis_topic).await;
    }

    let file_appender = RollingFileAppender::new(Rotation::DAILY, "", &args.log_file);
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let file_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(file_appender)
        .with_ansi(false);
    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stdout)
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(move || {
            struct Writer(std::io::Stdout);
            impl std::io::Write for Writer {
                fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                    print!("\r");
                    self.0.write(buf)
                }
                fn flush(&mut self) -> io::Result<()> {
                    self.0.flush()
                }
            }
            Writer(std::io::stdout())
        });
    tracing_subscriber::registry().with(env_filter).with(file_layer).with(stdout_layer).init();

    let stdin = io::stdin();
    let reader = stdin.lock();

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

/// Line seepd mode with just forwarding whatever received
async fn line_speed(redis_url: &str, redis_topic: &str) -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let reader = stdin.lock();

    let redis_client = redis::Client::open(redis_url)?;
    let mut conn = redis_client.get_multiplexed_async_connection().await?;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            break;
        }
        let publish_result: RedisResult<String> = conn.publish(redis_topic, line).await;
        if let Err(e) = publish_result {
            error!("Failed to publish to redis ({}): {}", redis_url, e);
        }
    }
    Ok(())
}
