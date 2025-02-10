use std::io::{self, BufRead};
use clap::Parser;
use redis::{AsyncCommands, RedisResult};
use tracing::error;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long, default_value = "redis://127.0.0.1:6379")]
    redis_url: String,

    #[arg(long, default_value = "line-speed")]
    redis_topic: String,

    #[arg(long = "expr", value_parser = parse_expression)]
    expressions: Vec<MatchExpression>,
}

#[derive(Debug, Clone)]
enum MatchExpression {
    Text(String),
    And(Vec<MatchExpression>),
    Or(Vec<MatchExpression>),
    Not(Box<MatchExpression>),
}

fn parse_expression(s: &str) -> Result<MatchExpression, String> {
    parse_expr(&mut s.chars().collect::<Vec<_>>())
}

fn parse_expr(chars: &mut Vec<char>) -> Result<MatchExpression, String> {
    // 跳过空白字符
    while chars.first().map_or(false, |c| c.is_whitespace()) {
        chars.remove(0);
    }

    match chars.first() {
        Some('(') => parse_group(chars),
        Some('!') => parse_not(chars),
        Some('"') => parse_quoted_text(chars),
        Some(c) if c.is_alphanumeric() => parse_text(chars),
        Some(c) => Err(format!("Unexpected character: {}", c)),
        None => Err("Empty expression".to_string()),
    }
}

/// 修改后的 parse_group 函数，支持多个操作数
fn parse_group(chars: &mut Vec<char>) -> Result<MatchExpression, String> {
    // 移除开括号
    chars.remove(0);
    let mut expressions = Vec::new();
    let mut op: Option<String> = None;

    loop {
        if chars.is_empty() {
            return Err("Unclosed parenthesis".to_string());
        }

        // 跳过空白字符
        while chars.first().map_or(false, |c| c.is_whitespace()) {
            chars.remove(0);
        }

        // 检查是否遇到闭括号
        if chars.first() == Some(&')') {
            chars.remove(0);
            break;
        }

        // 解析表达式
        let expr = parse_expr(chars)?;
        expressions.push(expr);

        // 跳过空白字符
        while chars.first().map_or(false, |c| c.is_whitespace()) {
            chars.remove(0);
        }

        // 如果遇到闭括号，结束解析
        if chars.first() == Some(&')') {
            chars.remove(0);
            break;
        }

        // 读取操作符
        let mut operator = String::new();
        while chars.first().map_or(false, |c| c.is_alphabetic()) {
            operator.push(chars.remove(0));
        }

        match operator.as_str() {
            "AND" | "OR" => {
                if let Some(prev_op) = &op {
                    if prev_op != &operator {
                        return Err("Mixed AND/OR operators not allowed in same group".to_string());
                    }
                } else {
                    op = Some(operator);
                }
            }
            "" => return Err("Expected operator AND or OR".to_string()),
            _ => return Err(format!("Invalid operator: {}", operator)),
        }
    }

    if expressions.is_empty() {
        return Err("Empty group".to_string());
    }

    match op {
        Some(op) if op == "AND" => Ok(MatchExpression::And(expressions)),
        Some(op) if op == "OR" => Ok(MatchExpression::Or(expressions)),
        None if expressions.len() == 1 => Ok(expressions.remove(0)),
        _ => Err("Invalid expression group".to_string()),
    }
}

fn parse_not(chars: &mut Vec<char>) -> Result<MatchExpression, String> {
    chars.remove(0); // 移除 '!'
    let expr = parse_expr(chars)?;
    Ok(MatchExpression::Not(Box::new(expr)))
}

fn parse_quoted_text(chars: &mut Vec<char>) -> Result<MatchExpression, String> {
    chars.remove(0); // 移除开引号
    let mut text = String::new();

    while let Some(c) = chars.first() {
        if *c == '"' {
            chars.remove(0);
            return Ok(MatchExpression::Text(text));
        }
        text.push(chars.remove(0));
    }

    Err("Unclosed quote".to_string())
}

fn parse_text(chars: &mut Vec<char>) -> Result<MatchExpression, String> {
    let mut text = String::new();
    while chars.first().map_or(false, |c| c.is_alphanumeric() || *c == '_') {
        text.push(chars.remove(0));
    }
    Ok(MatchExpression::Text(text))
}

fn evaluate_expression(text: &str, expr: &MatchExpression) -> bool {
    match expr {
        MatchExpression::Text(pattern) => text.contains(pattern),
        MatchExpression::And(expressions) => expressions.iter().all(|e| evaluate_expression(text, e)),
        MatchExpression::Or(expressions) => expressions.iter().any(|e| evaluate_expression(text, e)),
        MatchExpression::Not(expression) => !evaluate_expression(text, expression),
    }
}

async fn publish(conn: &mut redis::aio::MultiplexedConnection, redis_url: &str, redis_topic: &str, line: &str) {
    let publish_result: RedisResult<String> = conn.publish(redis_topic, line).await;
    if let Err(e) = publish_result {
        error!("Failed to publish to redis ({}): {}", redis_url, e);
    }
}

async fn line_speed(redis_url: &str, redis_topic: &str, expressions: Vec<MatchExpression>) -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let reader = stdin.lock();

    let redis_client = redis::Client::open(redis_url)?;
    let mut conn = redis_client.get_multiplexed_async_connection().await?;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            break;
        }
        if expressions.is_empty() || expressions.iter().any(|expr| evaluate_expression(&line, expr)) {
            publish(&mut conn, redis_url, redis_topic, &line).await;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    line_speed(&args.redis_url, &args.redis_topic, args.expressions).await?;
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_text() {
        let result = parse_expression("hello").unwrap();
        assert!(matches!(result, MatchExpression::Text(text) if text == "hello"));
    }

    #[test]
    fn test_parse_quoted_text() {
        let result = parse_expression("\"hello world\"").unwrap();
        assert!(matches!(result, MatchExpression::Text(text) if text == "hello world"));
    }

    #[test]
    fn test_parse_not() {
        let result = parse_expression("!hello").unwrap();
        assert!(matches!(result, MatchExpression::Not(_)));
    }

    #[test]
    fn test_parse_and_expression() {
        let result = parse_expression("(hello AND world)").unwrap();
        match result {
            MatchExpression::And(exprs) => {
                assert_eq!(exprs.len(), 2);
                assert!(matches!(&exprs[0], MatchExpression::Text(text) if text == "hello"));
                assert!(matches!(&exprs[1], MatchExpression::Text(text) if text == "world"));
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_parse_or_expression() {
        let result = parse_expression("(hello OR world)").unwrap();
        match result {
            MatchExpression::Or(exprs) => {
                assert_eq!(exprs.len(), 2);
                assert!(matches!(&exprs[0], MatchExpression::Text(text) if text == "hello"));
                assert!(matches!(&exprs[1], MatchExpression::Text(text) if text == "world"));
            }
            _ => panic!("Expected Or expression"),
        }
    }

    #[test]
    fn test_evaluate_simple_text() {
        let expr = MatchExpression::Text("hello".to_string());
        assert!(evaluate_expression("hello world", &expr));
        assert!(!evaluate_expression("world", &expr));
    }

    #[test]
    fn test_evaluate_and() {
        let expr = MatchExpression::And(vec![
            MatchExpression::Text("hello".to_string()),
            MatchExpression::Text("world".to_string()),
        ]);
        assert!(evaluate_expression("hello world", &expr));
        assert!(!evaluate_expression("hello", &expr));
    }

    #[test]
    fn test_evaluate_or() {
        let expr = MatchExpression::Or(vec![
            MatchExpression::Text("hello".to_string()),
            MatchExpression::Text("world".to_string()),
        ]);
        assert!(evaluate_expression("hello", &expr));
        assert!(evaluate_expression("world", &expr));
        assert!(!evaluate_expression("test", &expr));
    }

    #[test]
    fn test_evaluate_not() {
        let expr = MatchExpression::Not(Box::new(MatchExpression::Text("hello".to_string())));
        assert!(!evaluate_expression("hello world", &expr));
        assert!(evaluate_expression("world", &expr));
    }

    #[test]
    fn test_parse_complex_expression() {
        let result = parse_expression("(hello AND (world OR !test))").unwrap();
        // Complex structure validation
        match result {
            MatchExpression::And(exprs) => {
                assert_eq!(exprs.len(), 2);
                assert!(matches!(&exprs[0], MatchExpression::Text(text) if text == "hello"));
                match &exprs[1] {
                    MatchExpression::Or(or_exprs) => {
                        assert_eq!(or_exprs.len(), 2);
                        assert!(matches!(&or_exprs[0], MatchExpression::Text(text) if text == "world"));
                        assert!(matches!(&or_exprs[1], MatchExpression::Not(_)));
                    }
                    _ => panic!("Expected Or expression"),
                }
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_error_cases() {
        assert!(parse_expression("").is_err()); // Empty expression
        assert!(parse_expression("(hello").is_err()); // Unclosed parenthesis
        assert!(parse_expression("\"hello").is_err()); // Unclosed quote
        assert!(parse_expression("(hello XOR world)").is_err()); // Invalid operator
    }

    #[test]
        fn test_multiple_operands() {
            // 测试多个 AND 操作数
            let result = parse_expression("(a AND b AND c AND d)").unwrap();
            match result {
                MatchExpression::And(exprs) => {
                    assert_eq!(exprs.len(), 4);
                    assert!(matches!(&exprs[0], MatchExpression::Text(text) if text == "a"));
                    assert!(matches!(&exprs[1], MatchExpression::Text(text) if text == "b"));
                    assert!(matches!(&exprs[2], MatchExpression::Text(text) if text == "c"));
                    assert!(matches!(&exprs[3], MatchExpression::Text(text) if text == "d"));
                }
                _ => panic!("Expected And expression"),
            }

            // 测试多个 OR 操作数
            let result = parse_expression("(w OR x OR y OR z)").unwrap();
            match result {
                MatchExpression::Or(exprs) => {
                    assert_eq!(exprs.len(), 4);
                    assert!(matches!(&exprs[0], MatchExpression::Text(text) if text == "w"));
                    assert!(matches!(&exprs[1], MatchExpression::Text(text) if text == "x"));
                    assert!(matches!(&exprs[2], MatchExpression::Text(text) if text == "y"));
                    assert!(matches!(&exprs[3], MatchExpression::Text(text) if text == "z"));
                }
                _ => panic!("Expected Or expression"),
            }
        }

        #[test]
        fn test_complex_nested_expressions() {
            // 测试嵌套的多操作数表达式
            let result = parse_expression("(a AND b AND (x OR y OR z))").unwrap();
            match result {
                MatchExpression::And(exprs) => {
                    assert_eq!(exprs.len(), 3);
                    assert!(matches!(&exprs[0], MatchExpression::Text(text) if text == "a"));
                    assert!(matches!(&exprs[1], MatchExpression::Text(text) if text == "b"));
                    match &exprs[2] {
                        MatchExpression::Or(or_exprs) => {
                            assert_eq!(or_exprs.len(), 3);
                            assert!(matches!(&or_exprs[0], MatchExpression::Text(text) if text == "x"));
                            assert!(matches!(&or_exprs[1], MatchExpression::Text(text) if text == "y"));
                            assert!(matches!(&or_exprs[2], MatchExpression::Text(text) if text == "z"));
                        }
                        _ => panic!("Expected Or expression"),
                    }
                }
                _ => panic!("Expected And expression"),
            }
        }
}
