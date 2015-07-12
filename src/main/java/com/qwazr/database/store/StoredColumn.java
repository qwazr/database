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

import com.qwazr.database.model.ColumnDefinition;

import java.io.IOException;
import java.util.*;

public abstract class StoredColumn<T> extends ColumnAbstract<T> {

	private final StoreMapInterface<Integer, T> map;
	private final String collectionName;

	protected StoredColumn(String name, long columnId, StoreInterface store, ByteConverter<T> byteConverter) {
		super(name, columnId);
		collectionName = "store." + columnId;
		map = store.getMap(collectionName, ByteConverter.IntegerByteConverter.INSTANCE, byteConverter);
	}

	@Override
	public void deleteRow(Integer id) throws IOException {
		map.delete(id);
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
	public void collectValues(Iterator<Integer> docIds, ColumnValueCollector<T> collector) throws IOException {
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

	private static class StoredDoubleColumn extends StoredColumn<Double> {

		private StoredDoubleColumn(String name, long columnId, StoreInterface store) {
			super(name, columnId, store, ByteConverter.DoubleByteConverter.INSTANCE);
		}

		@Override
		final public Double convertValue(final Object value) {
			if (value instanceof Double)
				return (Double) value;
			return Double.valueOf(value.toString());
		}

	}

	private static class StoredStringColumn extends StoredColumn<String> {

		private StoredStringColumn(String name, long columnId, StoreInterface store) {
			super(name, columnId, store, ByteConverter.StringByteConverter.INSTANCE);
		}

		@Override
		final public String convertValue(final Object value) {
			if (value instanceof String)
				return (String) value;
			System.out.println("CONVERT VALUE: " + value.getClass());
			return value.toString();
		}

	}

	private static class StoredIntegerColumn extends StoredColumn<Integer> {

		private StoredIntegerColumn(String name, long columnId, StoreInterface store) {
			super(name, columnId, store, ByteConverter.IntegerByteConverter.INSTANCE);
		}

		@Override
		final public Integer convertValue(final Object value) {
			if (value instanceof Integer)
				return (Integer) value;
			return Integer.valueOf(value.toString());
		}

	}

	private static class StoredLongColumn extends StoredColumn<Long> {

		private StoredLongColumn(String name, long columnId, StoreInterface store) {
			super(name, columnId, store, ByteConverter.LongByteConverter.INSTANCE);
		}

		@Override
		final public Long convertValue(final Object value) {
			if (value instanceof Long)
				return (Long) value;
			return Long.valueOf(value.toString());
		}

	}

	static StoredColumn<?> newInstance(StoreInterface store, String columnName,
									   Integer columnId, ColumnDefinition.Type columnType)
			throws DatabaseException {
		switch (columnType) {
			case STRING:
				return new StoredStringColumn(columnName, columnId, store);
			case DOUBLE:
				return new StoredDoubleColumn(columnName, columnId, store);
			case LONG:
				return new StoredLongColumn(columnName, columnId, store);
			case INTEGER:
				return new StoredIntegerColumn(columnName, columnId, store);
			default:
				throw new DatabaseException("Unsupported type: " + columnType);
		}
	}

}
