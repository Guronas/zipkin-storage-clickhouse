create database if not exists zipkin;
create table if not exists zipkin.zipkin_spans
(
    trace_id            String,
    parent_id           String,
    id                  String,
    kind                Enum8('CLIENT'=0, 'SERVER'=1,'PRODUCER'=2,'CONSUMER'=3),
    name                String,
    timestamp           Int64,
    date_time           DateTime,
    duration            Int64,
    local_service_name  String,
    local_ipv4          String,
    local_ipv6          String,
    local_port          Int32,
    remote_service_name String,
    remote_ipv4         String,
    remote_ipv6         String,
    remote_port         Int32,
    annotations         Map(String, Int64),
    tags                Map(String, String),
    shared              UInt8,
    debug               UInt8
) engine = MergeTree
      ORDER BY (date_time, trace_id)
      PARTITION BY toYYYYMMDD(date_time)
      TTL date_time + INTERVAL 1 MONTH DELETE;