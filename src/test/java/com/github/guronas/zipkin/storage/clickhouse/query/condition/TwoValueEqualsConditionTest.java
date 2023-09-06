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

import com.github.guronas.zipkin.storage.clickhouse.query.ZipkinSpans;
import org.jooq.DSLContext;
import org.jooq.tools.jdbc.MockDataProvider;
import org.junit.jupiter.api.Test;

import static org.jooq.impl.DSL.inline;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static com.github.guronas.zipkin.storage.clickhouse.query.TestJooqUtils.getTestDSLContext;
import static com.github.guronas.zipkin.storage.clickhouse.query.condition.TwoValueEqualsCondition.twoValueEquals;

public class TwoValueEqualsConditionTest {
    private static final String EXPECT_TWO_VALUE_EQUALS_QUERY = "select * from testTable where `zipkin`.`zipkin_spans`.`trace_id` = 'testValue'";

    @Test
    public void twoValueEqualsConditionTest() {
        MockDataProvider mockDataProvider = ctx -> {
            assertEquals(EXPECT_TWO_VALUE_EQUALS_QUERY, ctx.sql());
            return null;
        };
        DSLContext dslContext = getTestDSLContext(mockDataProvider);
        dslContext.selectFrom("testTable")
                .where(twoValueEquals(ZipkinSpans.TRACE_ID, inline("testValue")))
                .fetch();
    }
}
