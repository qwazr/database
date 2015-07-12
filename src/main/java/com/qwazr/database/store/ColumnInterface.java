/**
 * Copyright 2015 Emmanuel Keller / QWAZR
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
package com.qwazr.database.store;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public interface ColumnInterface<T> {

	T convertValue(Object value);

	void setValues(Integer docId, Collection<Object> values) throws IOException;

	void setValue(Integer docId, Object value) throws IOException;

	T getValue(Integer docId) throws IOException;

	List<T> getValues(Integer docId) throws IOException;

	void collectValues(Iterator<Integer> docIds,
					   ColumnValueCollector<T> collector) throws IOException;

	void deleteRow(Integer id) throws IOException;

	void commit() throws IOException;

	void delete() throws IOException;

	public static interface ColumnValueCollector<T> {

		void collect(T value);
	}
}
