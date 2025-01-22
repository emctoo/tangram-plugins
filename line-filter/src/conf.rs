use std::collections::HashMap;

use redis::{AsyncCommands, RedisResult};
use serde::Deserialize;
use serde_json::Value;
use tracing::error;

use crate::eval::eval_on_flat_hash;
use crate::flatten::flatten_json;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub default: DefaultConfig,
    pub rules: Vec<Rule>,
}

#[derive(Debug, Deserialize)]
pub struct DefaultConfig {
    pub redis_url: String,
    pub redis_topic: String,
}

#[derive(Debug, Deserialize)]
pub struct Rule {
    pub expr: String,
    pub redis_url: Option<String>,
    pub redis_topic: Option<String>,
}

impl Rule {
    // 获取实际使用的 redis_url，如果没有配置则使用默认值
    pub fn get_redis_url<'a>(&'a self, default: &'a str) -> &'a str {
        self.redis_url.as_deref().unwrap_or(default)
    }

    // 获取实际使用的 redis_topic，如果没有配置则使用默认值
    pub fn get_redis_topic<'a>(&'a self, default: &'a str) -> &'a str {
        self.redis_topic.as_deref().unwrap_or(default)
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}

pub struct Dispatcher {
    config: Config,
    redis_clients: HashMap<String, redis::Client>,
}

impl Dispatcher {
    pub async fn new(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        let mut redis_clients = HashMap::new();

        // 添加默认 Redis 客户端
        redis_clients.insert(config.default.redis_url.clone(), redis::Client::open(config.default.redis_url.as_str())?);

        // 添加规则中的 Redis 客户端（去重）
        for rule in &config.rules {
            if let Some(url) = &rule.redis_url {
                if !redis_clients.contains_key(url) {
                    redis_clients.insert(url.clone(), redis::Client::open(url.as_str())?);
                }
            }
        }

        Ok(Self { config, redis_clients })
    }

    pub async fn process_message(&self, message: &str) -> Result<(), Box<dyn std::error::Error>> {
        let json: Value = serde_json::from_str(message)?;
        let mut flattened = HashMap::new();
        flatten_json(&json, "", &mut flattened);

        for rule in &self.config.rules {
            match eval_on_flat_hash(&flattened, &rule.expr) {
                Ok(result) if result => {
                    let redis_url = rule.get_redis_url(&self.config.default.redis_url);
                    let redis_topic = rule.get_redis_topic(&self.config.default.redis_topic);

                    if let Some(client) = self.redis_clients.get(redis_url) {
                        let mut conn = client.get_multiplexed_async_connection().await?;
                        let publish_result: RedisResult<String> = conn.publish(redis_topic, message).await;
                        if let Err(e) = publish_result {
                            error!("Failed to publish to redis ({}): {}", redis_url, e);
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => error!("Expression evaluation error: {}", e),
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_config_loading() -> Result<(), Box<dyn std::error::Error>> {
        let config_content = r#"
            [default]
            redis_url = "redis://localhost:6379"
            redis_topic = "default-topic"

            [[rules]]
            expr = "df == \"21\""
            redis_url = "redis://192.168.11.37:6379"
            redis_topic = "just-a-topic"

            [[rules]]
            expr = "user.age > 20"
            redis_topic = "adult-users"
        "#;

        let mut temp_file = NamedTempFile::new()?;
        write!(temp_file, "{}", config_content)?;

        let config = Config::load(temp_file.path().to_str().unwrap())?;

        assert_eq!(config.default.redis_url, "redis://localhost:6379");
        assert_eq!(config.default.redis_topic, "default-topic");
        assert_eq!(config.rules.len(), 2);

        let rule = &config.rules[0];
        assert_eq!(rule.expr, "df == \"21\"");
        assert_eq!(rule.redis_url.as_deref(), Some("redis://192.168.11.37:6379"));
        assert_eq!(rule.redis_topic.as_deref(), Some("just-a-topic"));

        let rule = &config.rules[1];
        assert_eq!(rule.expr, "user.age > 20");
        assert_eq!(rule.redis_url.as_deref(), None);
        assert_eq!(rule.redis_topic.as_deref(), Some("adult-users"));

        Ok(())
    }

    #[test]
    fn test_rule_defaults() {
        let rule = Rule {
            expr: "test".to_string(),
            redis_url: None,
            redis_topic: None,
        };

        assert_eq!(rule.get_redis_url("default_url"), "default_url");
        assert_eq!(rule.get_redis_topic("default_topic"), "default_topic");

        let rule = Rule {
            expr: "test".to_string(),
            redis_url: Some("custom_url".to_string()),
            redis_topic: Some("custom_topic".to_string()),
        };

        assert_eq!(rule.get_redis_url("default_url"), "custom_url");
        assert_eq!(rule.get_redis_topic("default_topic"), "custom_topic");
    }
}
