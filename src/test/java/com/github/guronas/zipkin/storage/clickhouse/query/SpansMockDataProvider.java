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

import org.jooq.*;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.MockDataProvider;
import org.jooq.tools.jdbc.MockExecuteContext;
import org.jooq.tools.jdbc.MockResult;
import zipkin2.Annotation;
import zipkin2.Endpoint;
import zipkin2.Span;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.github.guronas.zipkin.storage.clickhouse.query.ZipkinSpans.*;

public class SpansMockDataProvider implements MockDataProvider {
    private static final String SQL_TRACE_ID_PATTERN = """
            ^select distinct `zipkin`\\.`zipkin_spans`\\.`trace_id` \
            from `zipkin`\\.`zipkin_spans` \
            where \\(?`zipkin`\\.`zipkin_spans`\\.`date_time` between \\{ts '\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{0,3}'} \
            and \\{ts '\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{0,3}'}\\s?\
            (and `zipkin`\\.`zipkin_spans`\\.`local_service_name` = '\\S+')?\\s?\
            (and `zipkin`\\.`zipkin_spans`\\.`remote_service_name` = '\\S+')?\\s?\
            (and `zipkin`\\.`zipkin_spans`\\.`name` = '\\S+')?\\s?\
            (and mapContains\\(`zipkin`\\.`zipkin_spans`\\.`annotations`,'\\S+'\\s?\\))*\\s?\
            (and `zipkin`\\.`zipkin_spans`\\.`tags`\\['\\S+'] = '\\S+'\\s?)*\\s?\
            (and `zipkin`\\.`zipkin_spans`\\.`duration` >= \\d+)?\\s?\
            (and `zipkin`\\.`zipkin_spans`\\.`duration` <= \\d+)?\\s?\
            \\)? order by `zipkin`\\.`zipkin_spans`\\.`timestamp` desc limit \\d+$""";

    private static final String SQL_SELECT_SPANS_PATTERN = """
            ^select `zipkin`\\.`zipkin_spans`\\.`trace_id`, `zipkin`\\.`zipkin_spans`\\.`parent_id`, `zipkin`\\.`zipkin_spans`\\.`id`, \
            `zipkin`\\.`zipkin_spans`\\.`kind`, `zipkin`\\.`zipkin_spans`\\.`name`, `zipkin`\\.`zipkin_spans`\\.`timestamp`, \
            `zipkin`\\.`zipkin_spans`\\.`date_time`, `zipkin`\\.`zipkin_spans`\\.`duration`, `zipkin`\\.`zipkin_spans`\\.`local_service_name`, \
            `zipkin`\\.`zipkin_spans`\\.`local_ipv4`, `zipkin`\\.`zipkin_spans`\\.`local_ipv6`, `zipkin`\\.`zipkin_spans`\\.`local_port`, \
            `zipkin`\\.`zipkin_spans`\\.`remote_service_name`, `zipkin`\\.`zipkin_spans`\\.`remote_ipv4`, `zipkin`\\.`zipkin_spans`\\.`remote_ipv6`, \
            `zipkin`\\.`zipkin_spans`\\.`remote_port`, `zipkin`\\.`zipkin_spans`\\.`annotations`, `zipkin`\\.`zipkin_spans`\\.`tags`, \
            `zipkin`\\.`zipkin_spans`\\.`shared`, `zipkin`\\.`zipkin_spans`\\.`debug` \
            from `zipkin`\\.`zipkin_spans` where \\(?(`zipkin`\\.`zipkin_spans`\\.`date_time` between \\{ts '\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{0,3}'} \
            and \\{ts '\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{0,3}'} \
            and\\s)?`zipkin`\\.`zipkin_spans`\\.`trace_id` in \\(('.{16}',?\\s?)+\\)\\)?$""";

    private static final String SQL_TAGS_PATTERN = """
            ^select distinct `zipkin`\\.`zipkin_spans`\\.`tags`\\['(\\S+)'] as value \
            from `zipkin`\\.`zipkin_spans` where notEmpty\\(value\\)$""";

    private static final String SQL_INSERT_SPANS_PATTERN = """
            ^insert into `zipkin`\\.`zipkin_spans` \\((`local_ipv4`, `local_ipv6`, `local_port`,\\s)?(`remote_ipv4`, `remote_ipv6`, `remote_port`,\\s)?\
            `trace_id`, `parent_id`, `id`, `kind`, `name`, `timestamp`, `date_time`, `duration`, `local_service_name`, `remote_service_name`, \
            `annotations`, `tags`, `shared`, `debug`\\) \
            values (.+)+$""";

    private static final String SQL_INSERT_VALUES_PATTERN = """
            (('.*'|null), ('.*'|null), (\\d*|null),\\s){0,2}'.{16}', '.{16}', '.{16}', ('.*'|null), ('.*'|null), \\d*, \
            \\{ts '.*'}, \\d*, ('.*'|null), ('.*'|null), ('\\{('.*':('.*'|\\d*))*}',\\s){2}[01], [01]""";

    private static final String SQL_ANNOTATION_QUERY_PATTERN = """
            ^select distinct `zipkin`\\.`zipkin_spans`\\.`trace_id` \
            from `zipkin`\\.`zipkin_spans` \
            where \\(?`zipkin`\\.`zipkin_spans`\\.`date_time` between \\{ts '\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{0,3}'} \
            and \\{ts '\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{0,3}'}\\s?\
            (and \\(mapContains\\(`zipkin`\\.`zipkin_spans`\\.`annotations`,'\\S+'\\s?\\) \
            or mapContains\\(`zipkin`\\.`zipkin_spans`\\.`tags`,'\\S+'\\s?\\))\\)*\\s?\
            \\)? order by `zipkin`\\.`zipkin_spans`\\.`timestamp` desc limit \\d+$""";

    public static final Map<String, Span> TEST_SPANS = fillUpSpans();

    private final DSLContext dslContext = DSL.using(SQLDialect.MYSQL);

    private static Map<String, Span> fillUpSpans() {
        Map<String, Span> testSpans = new HashMap<>();
        testSpans.put("03c9304e40394d40", createTestSpan("03c9304e40394d40", "generalSpan").build());

        Span.Builder certainLocalServiceSpan = createTestSpan("03c9304e40394d41", "certainLocalServiceSpan");
        Endpoint localEndpoint = certainLocalServiceSpan.localEndpoint()
                .toBuilder()
                .serviceName("certainLocalServiceName")
                .build();
        certainLocalServiceSpan.localEndpoint(localEndpoint);
        testSpans.put("03c9304e40394d41", certainLocalServiceSpan.build());

        Span.Builder certainRemoteServiceSpan = createTestSpan("03c9304e40394d42", "certainRemoteServiceSpan");
        Endpoint remoteEndpoint = createEndpoint(null, "2.2.2.3", "2:2:2:2:2:2:2:3", 323);
        certainRemoteServiceSpan.remoteEndpoint(remoteEndpoint);
        testSpans.put("03c9304e40394d42", certainRemoteServiceSpan.build());

        Span.Builder certainSpanNameSpan = createTestSpan("03c9304e40394d43", "certainSpanName");
        testSpans.put("03c9304e40394d43", certainSpanNameSpan.build());

        Span.Builder annotationSpan = createTestSpan("03c9304e40394d44", "annotationSpan");
        annotationSpan.addAnnotation(1234567890001L, "certainAnnotation");
        testSpans.put("03c9304e40394d44", annotationSpan.build());

        Span.Builder tagsSpan = createTestSpan("03c9304e40394d45", "tagsSpan");
        tagsSpan.putTag("certainTag1", "certainValue1");
        tagsSpan.putTag("certainTag2", "certainValue2");
        testSpans.put("03c9304e40394d45", tagsSpan.build());

        Span.Builder certainDurationSpan = createTestSpan("03c9304e40394d46", "certainDurationSpan");
        certainDurationSpan.duration(10_000L);
        testSpans.put("03c9304e40394d46", certainDurationSpan.build());

        Span.Builder spanWithoutParentId = createTestSpan("03c9304e40394d47", "spanWithoutParentId");
        spanWithoutParentId.parentId(null);
        testSpans.put("03c9304e40394d47", spanWithoutParentId.build());

        return testSpans;
    }

    private static Span.Builder createTestSpan(String traceId, String name) {
        Endpoint localEndpoint = createEndpoint("testLocalService", "1.1.1.1", "1:1:1:1:1:1:1:1", 123);
        Endpoint remoteEndpoint = createEndpoint("testRemoteService", "2.2.2.2", "2:2:2:2:2:2:2:2", 321);
        return Span.newBuilder()
                .traceId(traceId)
                .parentId("03c9304e40394d4c")
                .id(traceId)
                .kind(Span.Kind.SERVER)
                .name(name)
                .timestamp(1234567889999001L)
                .duration(100L)
                .localEndpoint(localEndpoint)
                .remoteEndpoint(remoteEndpoint)
                .putTag("testKey1", "testValue1")
                .putTag("testKey2", "testValue2")
                .addAnnotation(1234567889999001L, "testAnnotation")
                .shared(true)
                .debug(true);
    }

    private static Endpoint createEndpoint(String name, String ipv4, String ipv6, int port) {
        return Endpoint.newBuilder()
                .serviceName(name)
                .ip(ipv4)
                .ip(ipv6)
                .port(port)
                .build();
    }

    @Override
    public MockResult[] execute(MockExecuteContext ctx) {
        String sql = ctx.sql();
        Object[] bindings = ctx.bindings();
        sql = dslContext.query(sql, bindings).toString();

        //Check sql query and timestamp bounds
        if (sql.matches(SQL_TRACE_ID_PATTERN) || sql.matches(SQL_ANNOTATION_QUERY_PATTERN)) {
            if (bindings[0].equals(Timestamp.valueOf("2009-02-14 02:31:29.999")) && bindings[1].equals(Timestamp.valueOf("2009-02-14 02:31:30.0"))) {
                return createTraceIdsResult(sql);
            } else {
                return new MockResult[]{new MockResult(0, dslContext.newResult(TRACE_ID))};
            }
        } else if (sql.matches(SQL_SELECT_SPANS_PATTERN)) {
            return createSelectSpansResult(bindings);
        } else if (sql.matches(SQL_TAGS_PATTERN)) {
            return createTagsResult(sql);
        } else if (sql.matches(SQL_INSERT_SPANS_PATTERN) && matchesValuesPattern(sql)) {
            return createSpansInsertResult(sql);
        } else {
            throw new IllegalArgumentException("Incorrect sql query %s".formatted(sql));
        }
    }

    private MockResult[] createTraceIdsResult(String sql) {
        Result<Record1<String>> result = dslContext.newResult(TRACE_ID);
        Span span;
        if (sql.contains("`local_service_name`")) {
            span = TEST_SPANS.get("03c9304e40394d41");
        } else if (sql.contains("`remote_service_name`")) {
            span = TEST_SPANS.get("03c9304e40394d42");
        } else if (sql.contains("`name`")) {
            span = TEST_SPANS.get("03c9304e40394d43");
        } else if (sql.contains("`annotations`")) {
            span = TEST_SPANS.get("03c9304e40394d44");
        } else if (sql.contains("`tags`")) {
            span = TEST_SPANS.get("03c9304e40394d45");
        } else if (sql.contains("`duration`")) {
            span = TEST_SPANS.get("03c9304e40394d46");
        } else {
            TEST_SPANS.values()
                    .forEach(s -> result.add(dslContext.newRecord(TRACE_ID).value1(s.traceId())));
            return new MockResult[]{new MockResult(TEST_SPANS.size(), result)};
        }

        result.add(dslContext.newRecord(TRACE_ID).value1(span.traceId()));
        return new MockResult[]{new MockResult(1, result)};
    }

    private MockResult[] createSelectSpansResult(Object[] bindings) {
        Result<Record> result = dslContext.newResult(ZIPKIN_SPANS_TABLE);
        //This is for cases when we have date_time range in condition
        int startIndex = bindings[0] instanceof Timestamp ? 2 : 0;
        for (int i = startIndex; i < bindings.length; i++) {
            Object binding = bindings[i];
            Span span = TEST_SPANS.get((String) binding);
            result.add(createRecord(span));
        }

        return new MockResult[]{new MockResult(3, result)};
    }

    private MockResult[] createTagsResult(String sql) {
        Pattern pattern = Pattern.compile(SQL_TAGS_PATTERN);
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find() && (matcher.group(1)).equals("certainTag1")) {
            Field<String> field = DSL.field("value", String.class);
            Result<Record1<String>> result = dslContext.newResult(field);
            Record1<String> record1 = dslContext.newRecord(field);
            record1.value1("certainValue1");
            Record1<String> record2 = dslContext.newRecord(field);
            record2.value1("certainValue2");
            Record1<String> record3 = dslContext.newRecord(field);
            record3.value1("certainValue3");
            result.add(record1);
            result.add(record2);
            result.add(record3);
            return new MockResult[]{new MockResult(3, result)};
        } else {
            throw new IllegalArgumentException("Unable to find a proper tag key in sql: " + sql);
        }
    }

    private MockResult[] createSpansInsertResult(String sql) {
        long insertedSpansCount = TEST_SPANS.values()
                .stream()
                .map(Span::traceId)
                .filter(sql::contains)
                .count();
        return new MockResult[]{new MockResult((int) insertedSpansCount)};
    }

    private boolean matchesValuesPattern(String sql) {
        String valuesExpression = "values ";
        int valuesIndex = sql.lastIndexOf(valuesExpression);
        return Arrays.stream(sql.substring(valuesIndex + valuesExpression.length())
                        .replaceFirst("\\(", "")
                        .replaceFirst("\\)$", "")
                        .split("\\), \\("))
                .allMatch(values -> values.matches(SQL_INSERT_VALUES_PATTERN));
    }

    private Record createRecord(Span span) {
        DSLContext dslContext = DSL.using(SQLDialect.MYSQL);
        Record record = dslContext.newRecord(ZIPKIN_SPANS_TABLE.fields());
        Endpoint localEndpoint = span.localEndpoint();
        Endpoint remoteEndpoint = span.remoteEndpoint();
        Map<String, Long> annotations = span.annotations()
                .stream()
                .collect(Collectors.toMap(Annotation::value, Annotation::timestamp));
        record.fromArray(span.traceId(), span.parentId(), span.id(), span.kind(), span.name(), span.timestamp(), Timestamp.from(Instant.now()),
                span.duration(), localEndpoint.serviceName(), localEndpoint.ipv4(), localEndpoint.ipv6(),
                localEndpoint.port(), remoteEndpoint.serviceName(), remoteEndpoint.ipv4(), remoteEndpoint.ipv6(),
                remoteEndpoint.port(), annotations, span.tags(), 1, 1);
        return record;
    }
}
