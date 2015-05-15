/**
 * Copyright 2015 Emmanuel Keller / QWAZR
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwarz.database;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public interface FieldInterface<T> {

	T convertValue(Object value);

	void setValues(Integer docId, Collection<Object> values) throws IOException;

	void setValue(Integer docId, Object value) throws IOException;

	T getValue(Integer docId) throws IOException;

	List<T> getValues(Integer docId) throws IOException;

	void collectValues(Iterator<Integer> docIds,
			FieldValueCollector<T> collector) throws IOException;

	void deleteDocument(Integer id) throws IOException;

	void commit() throws IOException;

	void delete() throws IOException;

	public static class FieldDefinition {

		public static enum Type {
			STRING, DOUBLE;
		}

		public static enum Mode {
			INDEXED, STORED;
		}

		public final String name;
		public final Type type;
		public final Mode mode;

		public FieldDefinition() {
			this(null, null, null);
		}

		public FieldDefinition(String name, Type type, Mode mode) {
			this.name = name;
			this.type = type;
			this.mode = mode;
		}
	}

	public static interface FieldValueCollector<T> {

		void collect(T value);
	}
}
