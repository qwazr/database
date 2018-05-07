/*
 * Copyright 2015-2018 Emmanuel Keller / QWAZR
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.database.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.qwazr.database.store.KeyStore;
import com.qwazr.utils.CollectionsUtils;

import java.util.Map;
import java.util.Objects;

@JsonInclude(Include.NON_EMPTY)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NON_PRIVATE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        creatorVisibility = JsonAutoDetect.Visibility.NONE)
public class TableDefinition {

    final public KeyStore.Impl implementation;
    final public Map<String, ColumnDefinition> columns;

    public static final String ID_COLUMN_NAME = "$id$";

    @JsonIgnore
    private volatile int hashCode;

    @JsonCreator
    public TableDefinition(@JsonProperty("implementation") final KeyStore.Impl implementation,
                           @JsonProperty("columns") final Map<String, ColumnDefinition> columns) {
        this.implementation = implementation;
        this.columns = columns;
    }

    public KeyStore.Impl getImplementation() {
        return implementation;
    }

    public Map<String, ColumnDefinition> getColumns() {
        return columns;
    }

    public synchronized int hashCode() {
        if (hashCode == 0) {
            int result = Objects.hashCode(implementation);
            if (columns != null)
                for (final String column : columns.keySet())
                    result = 31 * result + Objects.hashCode(column);
            hashCode = result;
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TableDefinition))
            return false;
        if (o == this)
            return true;
        final TableDefinition t = (TableDefinition) o;
        return Objects.equals(implementation, t.implementation) && CollectionsUtils.equals(columns, t.columns);
    }
}