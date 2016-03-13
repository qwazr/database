/**
 * Copyright 2015-2016 Emmanuel Keller / QWAZR
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.qwazr.utils.json.JsonMapper;

import java.io.IOException;
import java.util.Map;

public class ColumnDefinition {

	public enum Type {
		STRING, DOUBLE, LONG, INTEGER
	}

	public enum Mode {
		INDEXED, STORED
	}

	public final Type type;
	public final Mode mode;

	public ColumnDefinition() {
		this(null, null);
	}

	public ColumnDefinition(Type type, Mode mode) {
		this.type = type;
		this.mode = mode;
	}

	public ColumnDefinition(ColumnDefinition colDef) {
		if (colDef != null) {
			this.type = colDef.type;
			this.mode = colDef.mode;
		} else {
			this.type = null;
			this.mode = null;
		}
	}

	public static class Internal extends ColumnDefinition {

		public final int column_id;

		public Internal() {
			this(null, 0);
		}

		public Internal(ColumnDefinition colDef, int column_id) {
			super(colDef);
			this.column_id = column_id;
		}

	}

	public final static TypeReference<Map<String, ColumnDefinition>> MapStringColumnTypeRef = new TypeReference<Map<String, ColumnDefinition>>() {
	};

	public final static ColumnDefinition newColumnDefinition(String jsonString) throws IOException {
		return JsonMapper.MAPPER.readValue(jsonString, ColumnDefinition.class);
	}
}
