/**
 * Copyright 2015 OpenSearchServer Inc.
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

public interface FieldInterface {

	void setValues(Integer docId, Collection<String> values) throws IOException;

	void setValue(Integer docId, String value) throws IOException;

	List<String> getValues(Integer docId) throws IOException;

	void collectValues(Iterator<Integer> docIds, FieldValueCollector collector)
			throws IOException;

	void deleteDocument(Integer id) throws IOException;

	void commit() throws IOException;

	void delete() throws IOException;

	public static class FieldDefinition {

		public static enum Type {
			INDEXED, STORED;
		}

		public final String name;
		public final Type type;

		public FieldDefinition() {
			this(null, null);
		}

		public FieldDefinition(String name, Type type) {
			this.name = name;
			this.type = type;
		}
	}

	public static interface FieldValueCollector {

		void collect(String value);
	}
}
