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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.qwazr.database.annotations.TableColumn;
import com.qwazr.utils.ObjectMappers;

import javax.ws.rs.core.GenericType;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NON_PRIVATE,
        getterVisibility = JsonAutoDetect.Visibility.NONE,
        setterVisibility = JsonAutoDetect.Visibility.NONE,
        isGetterVisibility = JsonAutoDetect.Visibility.NONE,
        creatorVisibility = JsonAutoDetect.Visibility.NONE)
public class ColumnDefinition {

    public enum Type {
        STRING, DOUBLE, LONG, INTEGER
    }

    public enum Mode {
        INDEXED, STORED
    }

    public final Type type;
    public final Mode mode;

    @JsonIgnore
    private final int hashCode;

    @JsonCreator
    public ColumnDefinition(@JsonProperty("type") final Type type, @JsonProperty("mode") final Mode mode) {
        this.type = type;
        this.mode = mode;
        hashCode = Objects.hash(type, mode);
    }

    public ColumnDefinition(final TableColumn propertyField) {
        this(propertyField.type(), propertyField.mode());
    }

    public ColumnDefinition(final ColumnDefinition colDef) {
        if (colDef != null) {
            this.type = colDef.type;
            this.mode = colDef.mode;
        } else {
            this.type = null;
            this.mode = null;
        }
        hashCode = Objects.hash(type, mode);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ColumnDefinition))
            return false;
        if (o == this)
            return true;
        final ColumnDefinition c = (ColumnDefinition) o;
        return Objects.equals(type, c.type) && Objects.equals(mode, c.mode);
    }

    public Type getType() {
        return type;
    }

    public Mode getMode() {
        return mode;
    }

    public final static GenericType<Map<String, ColumnDefinition>> mapStringColumnType =
            new GenericType<Map<String, ColumnDefinition>>() {
            };

    public static ColumnDefinition newColumnDefinition(String jsonString) throws IOException {
        return ObjectMappers.JSON.readValue(jsonString, ColumnDefinition.class);
    }

    public final static ColumnDefinition ID_COLUMN_DEF = new ColumnDefinition(Type.STRING, Mode.INDEXED);

}
