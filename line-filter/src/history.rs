use redis::{RedisResult, AsyncCommands};
use serde::{Serialize, Deserialize};
use std::time::{SystemTime, UNIX_EPOCH};

const RETENTION_TIME: u64 = 30 * 60 * 1000; // 30minutes, 单位毫秒
const DUPLICATE_POLICY: &str = "LAST";  // 相同时间戳采用最新值
const COMPRESSION_POLICY: &str = "COMPRESSED"; // 使用压缩存储

#[derive(Debug, Serialize, Deserialize)]
pub struct Position {
    timestamp: i64,    // 毫秒时间戳
    latitude: f64,
    longitude: f64,
}

/// 位置时间序列管理器
pub struct PositionTimeSeriesTracker {
    redis_client: redis::Client,
}

impl PositionTimeSeriesTracker {
    pub fn new(redis_url: &str) -> RedisResult<Self> {
        let redis_client = redis::Client::open(redis_url)?;
        Ok(Self { redis_client })
    }

    /// 获取当前毫秒时间戳
    fn current_timestamp_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    /// 生成位置数据的时间序列 key
    fn ts_key(icao24: &str, field: &str) -> String {
        format!("aircraft:ts:{}:{}", icao24, field)
    }

    /// 创建时间序列
    async fn create_timeseries(&self, key: &str) -> RedisResult<()> {
        let mut conn = self.redis_client.get_async_connection().await?;

        // 使用 TS.CREATE 命令创建时间序列，如果已存在则忽略错误
        let result: RedisResult<()> = redis::cmd("TS.CREATE")
            .arg(key)
            .arg("RETENTION")
            .arg(RETENTION_TIME)
            .arg("DUPLICATE_POLICY")
            .arg(DUPLICATE_POLICY)
            .arg("ENCODING")
            .arg(COMPRESSION_POLICY)
            .query_async(&mut conn)
            .await;

        // 忽略已存在的错误
        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.to_string().contains("already exists") {
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    /// 记录新的位置
    pub async fn record_position(
        &self,
        icao24: &str,
        latitude: f64,
        longitude: f64
    ) -> RedisResult<()> {
        let mut conn = self.redis_client.get_async_connection().await?;
        let timestamp = Self::current_timestamp_ms();

        // 生成 key
        let lat_key = Self::ts_key(icao24, "lat");
        let lng_key = Self::ts_key(icao24, "lng");

        // 确保时间序列存在
        self.create_timeseries(&lat_key).await?;
        self.create_timeseries(&lng_key).await?;

        // 使用 pipeline 添加数据点
        redis::pipe()
            .atomic()
            // 添加纬度数据
            .cmd("TS.ADD")
            .arg(&lat_key)
            .arg(timestamp)
            .arg(latitude)
            // 添加经度数据
            .cmd("TS.ADD")
            .arg(&lng_key)
            .arg(timestamp)
            .arg(longitude)
            .query_async(&mut conn)
            .await?;

        Ok(())
    }

    /// 获取指定时间范围内的位置历史
    pub async fn get_position_history(
        &self,
        icao24: &str,
        start_time: i64,  // 毫秒时间戳
        end_time: i64     // 毫秒时间戳
    ) -> RedisResult<Vec<Position>> {
        let mut conn = self.redis_client.get_async_connection().await?;

        let lat_key = Self::ts_key(icao24, "lat");
        let lng_key = Self::ts_key(icao24, "lng");

        // 使用 TS.RANGE 获取数据
        let lat_data: Vec<(i64, f64)> = redis::cmd("TS.RANGE")
            .arg(&lat_key)
            .arg(start_time)
            .arg(end_time)
            .query_async(&mut conn)
            .await?;

        let lng_data: Vec<(i64, f64)> = redis::cmd("TS.RANGE")
            .arg(&lng_key)
            .arg(start_time)
            .arg(end_time)
            .query_async(&mut conn)
            .await?;

        // 合并经纬度数据
        let mut positions = Vec::new();
        for ((timestamp, lat), (_, lng)) in lat_data.into_iter().zip(lng_data.into_iter()) {
            positions.push(Position {
                timestamp,
                latitude: lat,
                longitude: lng,
            });
        }

        Ok(positions)
    }

    /// 获取最新位置
    pub async fn get_latest_position(&self, icao24: &str) -> RedisResult<Option<Position>> {
        let mut conn = self.redis_client.get_async_connection().await?;

        let lat_key = Self::ts_key(icao24, "lat");
        let lng_key = Self::ts_key(icao24, "lng");

        // 使用 TS.GET 获取最新数据点
        let lat_data: Option<(i64, f64)> = redis::cmd("TS.GET")
            .arg(&lat_key)
            .query_async(&mut conn)
            .await?;

        let lng_data: Option<(i64, f64)> = redis::cmd("TS.GET")
            .arg(&lng_key)
            .query_async(&mut conn)
            .await?;

        match (lat_data, lng_data) {
            (Some((timestamp, lat)), Some((_, lng))) => {
                Ok(Some(Position {
                    timestamp,
                    latitude: lat,
                    longitude: lng,
                }))
            }
            _ => Ok(None)
        }
    }

    /// 获取聚合数据
    pub async fn get_aggregated_positions(
        &self,
        icao24: &str,
        start_time: i64,
        end_time: i64,
        bucket_size_ms: u64,
        agg_type: &str  // 如 "avg", "min", "max"
    ) -> RedisResult<Vec<Position>> {
        let mut conn = self.redis_client.get_async_connection().await?;

        let lat_key = Self::ts_key(icao24, "lat");
        let lng_key = Self::ts_key(icao24, "lng");

        // 使用 TS.RANGE 获取聚合数据
        let lat_data: Vec<(i64, f64)> = redis::cmd("TS.RANGE")
            .arg(&lat_key)
            .arg(start_time)
            .arg(end_time)
            .arg("AGGREGATION")
            .arg(agg_type)
            .arg(bucket_size_ms)
            .query_async(&mut conn)
            .await?;

        let lng_data: Vec<(i64, f64)> = redis::cmd("TS.RANGE")
            .arg(&lng_key)
            .arg(start_time)
            .arg(end_time)
            .arg("AGGREGATION")
            .arg(agg_type)
            .arg(bucket_size_ms)
            .query_async(&mut conn)
            .await?;

        // 合并聚合数据
        let mut positions = Vec::new();
        for ((timestamp, lat), (_, lng)) in lat_data.into_iter().zip(lng_data.into_iter()) {
            positions.push(Position {
                timestamp,
                latitude: lat,
                longitude: lng,
            });
        }

        Ok(positions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[tokio::test]
    async fn test_position_tracking() -> RedisResult<()> {
        let tracker = PositionTimeSeriesTracker::new("redis://127.0.0.1:6379")?;
        let icao24 = "TEST123";

        // 记录几个位置点
        tracker.record_position(icao24, 39.9042, 116.4074).await?;
        thread::sleep(Duration::from_millis(100));
        tracker.record_position(icao24, 39.9142, 116.4174).await?;

        // 测试获取最新位置
        if let Some(position) = tracker.get_latest_position(icao24).await? {
            assert_eq!(position.latitude, 39.9142);
            assert_eq!(position.longitude, 116.4174);
        }

        // 测试获取历史记录
        let end_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let start_time = end_time - 1000; // 1秒前
        let history = tracker.get_position_history(icao24, start_time, end_time).await?;
        assert_eq!(history.len(), 2);

        // 测试聚合数据
        let agg_data = tracker.get_aggregated_positions(
            icao24,
            start_time,
            end_time,
            500, // 500ms 一个桶
            "avg"
        ).await?;
        assert!(!agg_data.is_empty());

        Ok(())
    }
}
