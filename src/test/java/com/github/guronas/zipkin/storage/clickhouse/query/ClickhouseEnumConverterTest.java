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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import zipkin2.Span;

import static org.junit.jupiter.api.Assertions.*;

public class ClickhouseEnumConverterTest {
    private final ClickHouseEnumConverter<Span.Kind> converter = new ClickHouseEnumConverter<>(Span.Kind.class);

    @ParameterizedTest
    @EnumSource(value = Span.Kind.class)
    public void fromConversionTest(Span.Kind expectedKind) {
        String name = expectedKind.name();
        Span.Kind kind = converter.from(name);
        assertEquals(expectedKind, kind);
    }

    @Test
    public void fromConversionWithUnexpectedValueTest() {
        String name = "unexpectedName";
        assertThrows(IllegalArgumentException.class, () -> converter.from(name));
    }

    @Test
    public void fromConversionWithNullValueTest() {
        Span.Kind kind = converter.from(null);
        assertNull(kind);
    }

    @ParameterizedTest
    @EnumSource(value = Span.Kind.class)
    public void toConversionTest(Span.Kind expectedKind) {
        String expectedName = expectedKind.name();
        String name = converter.to(expectedKind);
        assertEquals(expectedName, name);
    }

    @Test
    public void toConversionWithNullValueTest() {
        String name = converter.to(null);
        assertNull(name);
    }
}
