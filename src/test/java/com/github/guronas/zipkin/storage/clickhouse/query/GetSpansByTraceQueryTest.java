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

import org.junit.jupiter.api.Test;
import zipkin2.Span;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static com.github.guronas.zipkin.storage.clickhouse.query.SpansMockDataProvider.TEST_SPANS;

public class GetSpansByTraceQueryTest extends AbstractClickHouseQueryTest {

    public GetSpansByTraceQueryTest() {
        super(new SpansMockDataProvider());
    }

    @Test
    public void spansByTraceQueryTest() {
        List<String> traces = List.of("03c9304e40394d40", "03c9304e40394d41", "03c9304e40394d42");
        GetSpansByTraceQuery query = new GetSpansByTraceQuery(dslContext, traces);
        List<Span> spans = query.get();
        List<Span> expectedSpans = traces.stream()
                .map(TEST_SPANS::get)
                .collect(Collectors.toList());
        assertEquals(expectedSpans, spans);
    }
}
