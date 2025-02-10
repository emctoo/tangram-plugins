use std::io::{self, BufRead};
use clap::Parser;
use redis::{AsyncCommands, RedisResult};

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
    let mut tokens = tokenize(s)?;
    tokens.reverse(); // 反转令牌以便从前往后弹出
    parse_expr(&mut tokens)
}

fn tokenize(s: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut chars: Vec<char> = s.chars().collect();

    while !chars.is_empty() {
        // 跳过空白字符
        while chars.first().map_or(false, |c| c.is_whitespace()) {
            chars.remove(0);
        }

        if chars.is_empty() {
            break;
        }

        match chars[0] {
            '(' | ')' => {
                tokens.push(chars.remove(0).to_string());
            }
            '"' => {
                chars.remove(0); // 移除开引号
                let mut text = String::new();
                while let Some(c) = chars.first() {
                    if *c == '"' {
                        chars.remove(0);
                        break;
                    }
                    text.push(chars.remove(0));
                }
                tokens.push(text);
            }
            _ => {
                let mut token = String::new();
                while let Some(c) = chars.first() {
                    if c.is_whitespace() || *c == '(' || *c == ')' {
                        break;
                    }
                    token.push(chars.remove(0));
                }
                if !token.is_empty() {
                    tokens.push(token);
                }
            }
        }
    }
    Ok(tokens)
}

fn parse_expr(tokens: &mut Vec<String>) -> Result<MatchExpression, String> {
    match tokens.pop() {
        None => Err("Unexpected end of expression".to_string()),
        Some(token) => {
            if token == "(" {
                // 读取操作符或文本
                let op = tokens.pop().ok_or("Expected operator or text after (")?;
                let mut exprs = Vec::new();

                match op.as_str() {
                    "AND" | "OR" => {
                        // 解析子表达式直到遇到右括号
                        while tokens.last().map_or(false, |t| t != ")") {
                            exprs.push(parse_expr(tokens)?);
                        }
                        // 移除右括号
                        tokens.pop();

                        if exprs.is_empty() {
                            return Err(format!("Empty {} expression", op));
                        }

                        match op.as_str() {
                            "AND" => Ok(MatchExpression::And(exprs)),
                            "OR" => Ok(MatchExpression::Or(exprs)),
                            _ => unreachable!(),
                        }
                    }
                    "NOT" => {
                        // NOT 只能有一个操作数
                        let expr = parse_expr(tokens)?;
                        match tokens.pop() {
                            Some(t) if t == ")" => Ok(MatchExpression::Not(Box::new(expr))),
                            _ => Err("Expected ) after NOT expression".to_string()),
                        }
                    }
                    _ => {
                        // 处理单个文本的情况：(text)
                        exprs.push(MatchExpression::Text(op));
                        while tokens.last().map_or(false, |t| t != ")") {
                            match tokens.pop() {
                                Some(text) => exprs.push(MatchExpression::Text(text)),
                                None => return Err("Unexpected end of expression".to_string()),
                            }
                        }
                        tokens.pop(); // 移除右括号

                        if exprs.len() == 1 {
                            Ok(exprs.pop().unwrap())
                        } else {
                            // 如果有多个文本，默认用 AND 连接
                            Ok(MatchExpression::And(exprs))
                        }
                    }
                }
            } else {
                Ok(MatchExpression::Text(token))
            }
        }
    }
}

fn evaluate_expression(text: &str, expr: &MatchExpression) -> bool {
    match expr {
        MatchExpression::Text(pattern) => text.contains(pattern),
        MatchExpression::And(expressions) => expressions.iter().all(|e| evaluate_expression(text, e)),
        MatchExpression::Or(expressions) => expressions.iter().any(|e| evaluate_expression(text, e)),
        MatchExpression::Not(expression) => !evaluate_expression(text, expression),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_text() {
        let result = parse_expression("(text)").unwrap();
        assert!(matches!(result, MatchExpression::Text(text) if text == "text"));
    }

    #[test]
    fn test_and_expression() {
        let result = parse_expression("(AND hello world)").unwrap();
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
    fn test_or_expression() {
        let result = parse_expression("(OR error warning critical)").unwrap();
        match result {
            MatchExpression::Or(exprs) => {
                assert_eq!(exprs.len(), 3);
                assert!(matches!(&exprs[0], MatchExpression::Text(text) if text == "error"));
                assert!(matches!(&exprs[1], MatchExpression::Text(text) if text == "warning"));
                assert!(matches!(&exprs[2], MatchExpression::Text(text) if text == "critical"));
            }
            _ => panic!("Expected Or expression"),
        }
    }

    #[test]
    fn test_not_expression() {
        let result = parse_expression("(NOT error)").unwrap();
        assert!(matches!(result, MatchExpression::Not(_)));
    }

    #[test]
    fn test_complex_expression() {
        let result = parse_expression("(AND error (OR warning critical) (NOT debug))").unwrap();
        match result {
            MatchExpression::And(exprs) => {
                assert_eq!(exprs.len(), 3);
                assert!(matches!(&exprs[0], MatchExpression::Text(text) if text == "error"));
                match &exprs[1] {
                    MatchExpression::Or(or_exprs) => {
                        assert_eq!(or_exprs.len(), 2);
                        assert!(matches!(&or_exprs[0], MatchExpression::Text(text) if text == "warning"));
                        assert!(matches!(&or_exprs[1], MatchExpression::Text(text) if text == "critical"));
                    }
                    _ => panic!("Expected Or expression"),
                }
                assert!(matches!(&exprs[2], MatchExpression::Not(_)));
            }
            _ => panic!("Expected And expression"),
        }
    }

    #[test]
    fn test_implicit_and() {
        let result = parse_expression("(text1 text2 text3)").unwrap();
        match result {
            MatchExpression::And(exprs) => {
                assert_eq!(exprs.len(), 3);
                assert!(matches!(&exprs[0], MatchExpression::Text(text) if text == "text1"));
                assert!(matches!(&exprs[1], MatchExpression::Text(text) if text == "text2"));
                assert!(matches!(&exprs[2], MatchExpression::Text(text) if text == "text3"));
            }
            _ => panic!("Expected And expression"),
        }
    }
}

#[tokio::main]
async fn main() -> RedisResult<()> {
    let cli = Cli::parse();
    let redis_client = redis::Client::open(cli.redis_url).unwrap();
    let mut con = redis_client.get_multiplexed_async_connection().await?;

    let mut line = String::new();
    let stdin = io::stdin();
    let mut handle = stdin.lock();

    while handle.read_line(&mut line).unwrap() > 0 {
        {
        let line = line.trim();
        if !line.is_empty() {
            let mut matched = false;
            for expr in &cli.expressions {
                if evaluate_expression(line, expr) {
                    matched = true;
                    break;
                }
            }
            if matched {
                let _: RedisResult<()> = con.publish(cli.redis_topic.clone(), line).await;
            }
        }
        }
        line.clear();
    }
    Ok(())
}
