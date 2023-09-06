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

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class AggregationByServiceNameQueryTest extends AbstractClickHouseQueryTest {

    @Test
    public void testSelectByServiceNameQuery() {
        AggregationByServiceNameQuery query = new AggregationByServiceNameQuery(dslContext, "testService", ZipkinSpans.NAME);
        List<String> values = query.get();
        assertEquals(List.of("testName1", "testName2"), values);
    }
}
