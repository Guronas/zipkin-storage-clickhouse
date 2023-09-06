/*
 * Copyright (c) 2023 Maksim Frolov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.guronas.zipkin.storage.clickhouse.query;

import org.jooq.Record;
import org.jooq.TableField;
import org.jooq.impl.DSL;
import org.jooq.impl.TableImpl;
import zipkin2.Span;

import java.sql.Timestamp;
import java.util.Map;

import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.schema;

public class ZipkinSpans extends TableImpl<Record> {
    public static final ZipkinSpans ZIPKIN_SPANS_TABLE = new ZipkinSpans();

    private ZipkinSpans() {
        super(name("zipkin_spans"), schema("zipkin"));
    }

    public static final TableField<Record, String> TRACE_ID = createField(DSL.name("trace_id"), ClickhouseDataType.STRING.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, String> PARENT_ID = createField(DSL.name("parent_id"), ClickhouseDataType.STRING.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, String> ID = createField(DSL.name("id"), ClickhouseDataType.STRING.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, Span.Kind> KIND = createField(DSL.name("kind"), ClickhouseDataType.enumDataType(Span.Kind.class).nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, String> NAME = createField(DSL.name("name"), ClickhouseDataType.STRING.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, Long> TIMESTAMP = createField(DSL.name("timestamp"), ClickhouseDataType.INT64.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, Timestamp> DATE_TIME = createField(DSL.name("date_time"), ClickhouseDataType.DATE_TIME.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, Long> DURATION = createField(DSL.name("duration"), ClickhouseDataType.INT64.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, String> LOCAL_SERVICE_NAME = createField(DSL.name("local_service_name"), ClickhouseDataType.STRING.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, String> LOCAL_IPV4 = createField(DSL.name("local_ipv4"), ClickhouseDataType.STRING.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, String> LOCAL_IPV6 = createField(DSL.name("local_ipv6"), ClickhouseDataType.STRING.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, Integer> LOCAL_PORT = createField(DSL.name("local_port"), ClickhouseDataType.INT32.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, String> REMOTE_SERVICE_NAME = createField(DSL.name("remote_service_name"), ClickhouseDataType.STRING.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, String> REMOTE_IPV4 = createField(DSL.name("remote_ipv4"), ClickhouseDataType.STRING.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, String> REMOTE_IPV6 = createField(DSL.name("remote_ipv6"), ClickhouseDataType.STRING.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, Integer> REMOTE_PORT = createField(DSL.name("remote_port"), ClickhouseDataType.INT32.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, Map<String, Long>> ANNOTATIONS = createField(DSL.name("annotations"), ClickhouseDataType.<String, Long>mapDataType().nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, Map<String, String>> TAGS = createField(DSL.name("tags"), ClickhouseDataType.<String, String>mapDataType().nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, Short> SHARED = createField(DSL.name("shared"), ClickhouseDataType.UINT8.nullable(false), ZIPKIN_SPANS_TABLE);

    public static final TableField<Record, Short> DEBUG = createField(DSL.name("debug"), ClickhouseDataType.UINT8.nullable(false), ZIPKIN_SPANS_TABLE);
}
