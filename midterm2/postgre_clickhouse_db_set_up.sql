----   PostgreSQL -----

CREATE TABLE IF NOT EXISTS metric_baselines (
    source_id VARCHAR(255),
    metric VARCHAR(100),
    mean_value FLOAT8,
    stddev_value FLOAT8,
    last_calculated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_id, metric)
);

CREATE TABLE IF NOT EXISTS drift_status (
    source_id VARCHAR(255),
    metric VARCHAR(100),
    status VARCHAR(20) DEFAULT 'NORMAL',
    current_value FLOAT8,
    drift_start_time TIMESTAMP NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_id, metric)
);



---  ClickHouse  ------


CREATE TABLE IF NOT EXISTS events_raw (
    source_id String,
    metric String,
    value Float64,
    event_time DateTime64(3, 'UTC'),
    processed_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (source_id, metric, event_time);



CREATE TABLE IF NOT EXISTS events_queue (
    source_id String,
    metric String,
    value Float64,
    event_time String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'localhost:9092,localhost:19092,localhost:29092',
    kafka_topic_list = 'telemetry.events',
    kafka_group_name = 'ch_consumer_group',
    kafka_format = 'JSONEachRow';




CREATE MATERIALIZED VIEW events_mv
TO events_raw AS
SELECT
    source_id,
    metric,
    value,
    parseDateTime64BestEffort(event_time) AS event_time
FROM events_queue;





CREATE TABLE IF NOT EXISTS drift_events (
    source_id String,
    metric String,
    drift_start DateTime,
    drift_end Nullable(DateTime),
    baseline_mean Float64,
    baseline_std Float64
)
ENGINE = MergeTree
ORDER BY (source_id, metric, drift_start);







