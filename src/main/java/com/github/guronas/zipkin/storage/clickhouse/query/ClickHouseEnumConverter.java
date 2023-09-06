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

import lombok.RequiredArgsConstructor;
import org.jooq.Converter;

import java.util.Optional;

@RequiredArgsConstructor
public class ClickHouseEnumConverter<T extends Enum<T>> implements Converter<String, T> {
    private final Class<T> enumClass;

    @Override
    public T from(String databaseObject) {
        return Optional.ofNullable(databaseObject)
                .filter(stringValue -> !stringValue.isEmpty())
                .map(stringValue -> T.valueOf(enumClass, stringValue))
                .orElse(null);
    }

    @Override
    public String to(T userObject) {
        return Optional.ofNullable(userObject)
                .map(T::name)
                .orElse(null);
    }

    @Override
    public Class<String> fromType() {
        return String.class;
    }

    @Override
    public Class<T> toType() {
        return enumClass;
    }
}
