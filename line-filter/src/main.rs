use std::collections::HashMap;
use std::io::{self, BufRead};

use evalexpr::{eval_boolean_with_context, ContextWithMutableVariables, EvalexprResult, HashMapContext};
use redis::{AsyncCommands, RedisResult};
use serde_json::Value;
use serde::Deserialize;
use tracing::error;
use clap::Parser;

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

struct Dispatcher {
    config: Config,
    redis_clients: HashMap<String, redis::Client>,
}

impl Dispatcher {
    async fn new(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        let mut redis_clients = HashMap::new();
        
        // 添加默认 Redis 客户端
        redis_clients.insert(
            config.default.redis_url.clone(),
            redis::Client::open(config.default.redis_url.as_str())?
        );

        // 添加规则中的 Redis 客户端（去重）
        for rule in &config.rules {
            if let Some(url) = &rule.redis_url {
                if !redis_clients.contains_key(url) {
                    redis_clients.insert(url.clone(), redis::Client::open(url.as_str())?);
                }
            }
        }

        Ok(Self {
            config,
            redis_clients,
        })
    }

    async fn process_message(&self, message: &str) -> Result<(), Box<dyn std::error::Error>> {
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
                Ok(_) => {},
                Err(e) => error!("Expression evaluation error: {}", e),
            }
        }

        Ok(())
    }
}


// 将 serde_json::Value 转换为 evalexpr::Value
fn json_value_to_eval_value(value: &Value) -> evalexpr::Value {
    match value {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                evalexpr::Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                evalexpr::Value::Float(f)
            } else {
                evalexpr::Value::Empty
            }
        }
        Value::String(s) => evalexpr::Value::String(s.clone()),
        Value::Bool(b) => evalexpr::Value::Boolean(*b),
        Value::Null => evalexpr::Value::Empty,
        _ => evalexpr::Value::Empty,
    }
}

fn eval_on_flat_hash(flat_map: &HashMap<String, Value>, expr: &str) -> EvalexprResult<bool> {
    let mut context = HashMapContext::new();

    // 将扁平化的 HashMap 转换为 evalexpr 上下文
    for (key, value) in flat_map {
        context.set_value(key.replace(".", "_"), json_value_to_eval_value(value))?;
    }

    // 替换表达式中的点号为下划线
    let modified_expr = expr.replace(".", "_");

    // 执行表达式并返回布尔结果
    eval_boolean_with_context(&modified_expr, &context)
}

fn flatten_json(obj: &Value, prefix: &str, result: &mut HashMap<String, Value>) {
    match obj {
        Value::Object(map) => {
            for (key, value) in map {
                let new_key = if prefix.is_empty() { key.clone() } else { format!("{}.{}", prefix, key) };
                flatten_json(value, &new_key, result);
            }
        }
        Value::Array(arr) => {
            for (i, value) in arr.iter().enumerate() {
                let new_key = if prefix.is_empty() {
                    format!("[{}]", i)
                } else {
                    format!("{}[{}]", prefix, i)
                };
                flatten_json(value, &new_key, result);
            }
        }
        _ => {
            if !prefix.is_empty() {
                result.insert(prefix.to_string(), obj.clone());
            }
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::NamedTempFile;
    use std::io::Write;

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

    #[test]
    fn test_flatten_simple_object() {
        let input = json!({
            "name": "John",
            "age": 30
        });

        let mut result = HashMap::new();
        flatten_json(&input, "", &mut result);

        assert_eq!(result.get("name").unwrap().as_str().unwrap(), "John");
        assert_eq!(result.get("age").unwrap().as_i64().unwrap(), 30);
    }

    #[test]
    fn test_flatten_nested_object() {
        let input = json!({
            "user": {
                "name": "John",
                "address": {
                    "city": "New York",
                    "zip": "10001"
                }
            }
        });

        let mut result = HashMap::new();
        flatten_json(&input, "", &mut result);

        assert_eq!(result.get("user.name").unwrap().as_str().unwrap(), "John");
        assert_eq!(result.get("user.address.city").unwrap().as_str().unwrap(), "New York");
        assert_eq!(result.get("user.address.zip").unwrap().as_str().unwrap(), "10001");
    }

    #[test]
    fn test_flatten_array() {
        let input = json!({
            "users": [
                {"id": 1, "name": "John"},
                {"id": 2, "name": "Jane"}
            ]
        });

        let mut result = HashMap::new();
        flatten_json(&input, "", &mut result);

        assert_eq!(result.get("users[0].id").unwrap().as_i64().unwrap(), 1);
        assert_eq!(result.get("users[0].name").unwrap().as_str().unwrap(), "John");
        assert_eq!(result.get("users[1].id").unwrap().as_i64().unwrap(), 2);
        assert_eq!(result.get("users[1].name").unwrap().as_str().unwrap(), "Jane");
    }

    #[test]
    fn test_flatten_mixed_types() {
        let input = json!({
            "id": 1,
            "data": {
                "numbers": [1, 2, 3],
                "active": true,
                "details": {
                    "note": "test"
                }
            }
        });

        let mut result = HashMap::new();
        flatten_json(&input, "", &mut result);

        assert_eq!(result.get("id").unwrap().as_i64().unwrap(), 1);
        assert_eq!(result.get("data.numbers[0]").unwrap().as_i64().unwrap(), 1);
        assert_eq!(result.get("data.numbers[1]").unwrap().as_i64().unwrap(), 2);
        assert_eq!(result.get("data.numbers[2]").unwrap().as_i64().unwrap(), 3);
        assert_eq!(result.get("data.active").unwrap().as_bool().unwrap(), true);
        assert_eq!(result.get("data.details.note").unwrap().as_str().unwrap(), "test");
    }

    #[test]
    fn test_flatten_empty_object() {
        let input = json!({});

        let mut result = HashMap::new();
        flatten_json(&input, "", &mut result);

        assert!(result.is_empty());
    }

    #[test]
    fn test_flatten_null_values() {
        let input = json!({
            "name": null,
            "data": {
                "value": null
            }
        });

        let mut result = HashMap::new();
        flatten_json(&input, "", &mut result);

        assert!(result.get("name").unwrap().is_null());
        assert!(result.get("data.value").unwrap().is_null());
    }

    #[test]
    fn test_flatten_with_prefix() {
        let input = json!({
            "name": "John",
            "age": 30
        });

        let mut result = HashMap::new();
        flatten_json(&input, "user", &mut result);

        assert_eq!(result.get("user.name").unwrap().as_str().unwrap(), "John");
        assert_eq!(result.get("user.age").unwrap().as_i64().unwrap(), 30);
    }

    #[test]
    fn test_flatten_special_characters() {
        let input = json!({
            "user.name": "John",
            "data": {
                "key.with.dots": "value"
            }
        });

        let mut result = HashMap::new();
        flatten_json(&input, "", &mut result);

        assert_eq!(result.get("user.name").unwrap().as_str().unwrap(), "John");
        assert_eq!(result.get("data.key.with.dots").unwrap().as_str().unwrap(), "value");
    }

    // use super::*;
    // use serde_json::json;

    #[test]
    fn test_simple_comparison() {
        let input = json!({
            "age": 25,
            "name": "John"
        });

        let mut flat_map = HashMap::new();
        flatten_json(&input, "", &mut flat_map);

        let result = eval_on_flat_hash(&flat_map, "age > 20").unwrap();
        assert!(result);

        let result = eval_on_flat_hash(&flat_map, "age < 20").unwrap();
        assert!(!result);
    }

    #[test]
    fn test_nested_fields() {
        let input = json!({
            "user": {
                "age": 25,
                "profile": {
                    "score": 85
                }
            }
        });

        let mut flat_map = HashMap::new();
        flatten_json(&input, "", &mut flat_map);

        let result = eval_on_flat_hash(&flat_map, "user_age >= 20 && user_profile_score > 80").unwrap();
        assert!(result);
    }

    #[test]
    fn test_array_fields() {
        let input = json!({
            "scores": [85, 90, 95],
            "name": "John"
        });

        let mut flat_map = HashMap::new();
        flatten_json(&input, "", &mut flat_map);

        let result = eval_on_flat_hash(&flat_map, "scores[0] >= 80 && scores[1] >= 90").unwrap();
        assert!(result);
    }

    #[test]
    fn test_string_comparison() {
        let input = json!({
            "user": {
                "name": "John",
                "role": "admin"
            }
        });

        let mut flat_map = HashMap::new();
        flatten_json(&input, "", &mut flat_map);

        let result = eval_on_flat_hash(&flat_map, "user_role == \"admin\"").unwrap();
        assert!(result);
    }

    #[test]
    fn test_complex_expression() {
        let input = json!({
            "user": {
                "age": 25,
                "active": true,
                "scores": [85, 90, 95]
            }
        });

        let mut flat_map = HashMap::new();
        flatten_json(&input, "", &mut flat_map);

        let result = eval_on_flat_hash(&flat_map, "user_age > 20 && user_active && user_scores[0] >= 80").unwrap();
        assert!(result);
    }

    #[test]
    fn test_invalid_expression() {
        let input = json!({
            "age": 25
        });

        let mut flat_map = HashMap::new();
        flatten_json(&input, "", &mut flat_map);

        let result = eval_on_flat_hash(&flat_map, "invalid_field > 20");
        assert!(result.is_err());
    }
}
