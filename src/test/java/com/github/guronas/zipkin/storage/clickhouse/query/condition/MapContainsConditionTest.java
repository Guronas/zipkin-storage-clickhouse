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

package com.github.guronas.zipkin.storage.clickhouse.query.condition;

import com.github.guronas.zipkin.storage.clickhouse.query.TestJooqUtils;
import com.github.guronas.zipkin.storage.clickhouse.query.ZipkinSpans;
import org.jooq.DSLContext;
import org.jooq.tools.jdbc.MockDataProvider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MapContainsConditionTest {
    private static final String EXPECT_MAP_CONTAINS_QUERY = "select * from testTable where mapContains(`zipkin`.`zipkin_spans`.`tags`,'testKey')";

    @Test
    public void mapContainsConditionTest() {
        MockDataProvider mockDataProvider = ctx -> {
            assertEquals(EXPECT_MAP_CONTAINS_QUERY, ctx.sql());
            return null;
        };
        DSLContext dslContext = TestJooqUtils.getTestDSLContext(mockDataProvider);
        dslContext.selectFrom("testTable")
                .where(MapContainsCondition.mapContains(ZipkinSpans.TAGS, "testKey"))
                .fetch();
    }

}
