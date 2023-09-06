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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import zipkin2.Span;
import zipkin2.storage.QueryRequest;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class GetAllSpansQueryTest extends AbstractClickHouseQueryTest {

    /**
     * We can't use here MockFileDatabase because jooq doesn't know about Map fields which are used in ClickHouse
     */
    public GetAllSpansQueryTest() {
        super(new SpansMockDataProvider());
    }

    @ParameterizedTest
    @MethodSource("getSelectSpansArgs")
    public void selectCertainSpansTest(String expectedTrace, QueryRequest queryRequest) {
        GetAllSpansQuery query = new GetAllSpansQuery(dslContext, queryRequest);
        List<Span> spans = query.get();
        assertEquals(1, spans.size());
        Assertions.assertEquals(SpansMockDataProvider.TEST_SPANS.get(expectedTrace), spans.get(0));
    }

    @Test
    public void selectAllSpansTest() {
        GetAllSpansQuery query = new GetAllSpansQuery(dslContext, createQueryRequest().build());
        List<Span> spans = query.get();
        assertEquals(new ArrayList<>(SpansMockDataProvider.TEST_SPANS.values()), spans);
    }

    @Test
    public void emptySpansResultTest() {
        QueryRequest queryRequest = createQueryRequest()
                .endTs(9234567890000L)
                .lookback(1L)
                .build();
        GetAllSpansQuery query = new GetAllSpansQuery(dslContext, queryRequest);
        List<Span> spans = query.get();
        assertTrue(spans.isEmpty());
    }

    public static Stream<Arguments> getSelectSpansArgs() {
        HashMap<String, String> annotations = new HashMap<>();
        annotations.put("certainAnnotation", null);
        HashMap<String, String> tags = new HashMap<>();
        tags.put("certainTag1", "certainValue1");
        tags.put("certainTag2", "certainValue2");
        return Stream.of(
                Arguments.arguments("03c9304e40394d41", createQueryRequest().serviceName("certainLocalServiceName").build()),
                Arguments.arguments("03c9304e40394d42", createQueryRequest().remoteServiceName("certainRemoteServiceName").build()),
                Arguments.arguments("03c9304e40394d43", createQueryRequest().spanName("certainSpanName").build()),
                Arguments.arguments("03c9304e40394d44", createQueryRequest().annotationQuery(annotations).build()),
                Arguments.arguments("03c9304e40394d45", createQueryRequest().annotationQuery(tags).build()),
                Arguments.arguments("03c9304e40394d46", createQueryRequest().minDuration(9_500L).maxDuration(10_500L).build())
        );
    }

    private static QueryRequest.Builder createQueryRequest() {
        return QueryRequest.newBuilder()
                .endTs(1234567890000L)
                .lookback(1L)
                .limit(10);
    }
}
