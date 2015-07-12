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
package com.qwazr.database;

import com.qwazr.database.store.ByteConverter;
import com.qwazr.database.store.StoreInterface;
import com.qwazr.database.store.StoreMapInterface;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class StoredColumn<T> extends ColumnAbstract<T> {

	private final StoreMapInterface<Integer, T> map;
	private final String collectionName;

	protected StoredColumn(String name, long columnId, StoreInterface storeDb,
						   AtomicBoolean wasExisting, ByteConverter<T> byteConverter) {
		super(name, columnId);
		collectionName = "store." + columnId;
		wasExisting.set(storeDb.exists(collectionName));
		map = storeDb.getMap(collectionName, ByteConverter.IntegerByteConverter.INSTANCE, byteConverter);
	}

	@Override
	public void deleteRow(Integer id) {
		map.remove(id);
	}

	@Override
	public void setValue(Integer id, Object value) throws IOException {
		map.put(id, convertValue(value));
	}

	@Override
	public void setValues(Integer docId, Collection<Object> values) {
		throw new IllegalArgumentException(
				"Only one value allowed for this field: " + this.name);
	}

	@Override
	public List<T> getValues(Integer docId) throws IOException {
		T value = map.get(docId);
		if (value == null)
			return Collections.emptyList();
		ArrayList<T> list = new ArrayList<T>(1);
		list.add(value);
		return list;
	}

	@Override
	public T getValue(Integer docId) throws IOException {
		return map.get(docId);
	}

	@Override
	public void collectValues(Iterator<Integer> docIds,
							  ColumnValueCollector<T> collector) throws IOException {
		Integer docId;
		try {
			while ((docId = docIds.next()) != null) {
				T value = map.get(docId);
				if (value == null)
					continue;
				collector.collect(value);
			}
		} catch (NoSuchElementException | ArrayIndexOutOfBoundsException e) {
			// Faster use the exception than calling hasNext for each document
		}
	}

	public static class StoredDoubleColumn extends StoredColumn<Double> {

		StoredDoubleColumn(String name, long columnId, StoreInterface storeDb,
						   AtomicBoolean wasExisting) {
			super(name, columnId, storeDb, wasExisting, ByteConverter.DoubleByteConverter.INSTANCE);
		}

		@Override
		final public Double convertValue(final Object value) {
			if (value instanceof Double)
				return (Double) value;
			return Double.valueOf(value.toString());
		}

	}

	public static class StoredStringColumn extends StoredColumn<String> {

		StoredStringColumn(String name, long columnId, StoreInterface storeDb,
						   AtomicBoolean wasExisting) {
			super(name, columnId, storeDb, wasExisting, ByteConverter.StringByteConverter.INSTANCE);
		}

		@Override
		final public String convertValue(final Object value) {
			if (value instanceof String)
				return (String) value;
			return value.toString();
		}

	}

}
