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
 **/
package com.qwazr.database.store.keys;

import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.store.ByteConverter;
import com.qwazr.database.store.DatabaseException;
import com.qwazr.database.store.KeyStore;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

public class ColumnIndexesKey<T, V> extends KeysAbstract<T, V> {

	final private ColumnDefinition.Internal colDef;
	final private ArrayIterator arrayIterator;

	public ColumnIndexesKey(ColumnDefinition.Internal colDef) throws DatabaseException {
		super(KeyEnum.COLUMN_INDEX);
		this.colDef = colDef;
		switch (colDef.type) {
		case STRING:
			arrayIterator = new StringArrayIterator();
			break;
		case LONG:
			arrayIterator = new LongArrayIterator();
			break;
		case INTEGER:
			arrayIterator = new IntArrayIterator();
			break;
		case DOUBLE:
			arrayIterator = new DoubleArrayIterator();
			break;
		default:
			throw new DatabaseException("Unsupported type: " + colDef.type);
		}
	}

	@Override
	final public void buildKey(final DataOutputStream output) throws IOException {
		super.buildKey(output);
		output.writeInt(colDef.column_id);
	}

	final public void remove(KeyStore store, ColumnStoreKey<?, ?> columnStoreKey) throws IOException {
		Object value = columnStoreKey.getValue(store);
		if (value == null)
			return;
		arrayIterator.remove(store, value, columnStoreKey.docId);
	}

	final public void select(KeyStore store, Object value, int docId) throws IOException {
		if (value instanceof Collection<?>) {
			for (Object val : (Collection<?>) value)
				ColumnIndexKey.newInstance(colDef, val).select(store, docId);
		} else if (value.getClass().isArray()) {
			for (Object val : (Object[]) value)
				ColumnIndexKey.newInstance(colDef, val).select(store, docId);
		} else
			ColumnIndexKey.newInstance(colDef, value).select(store, docId);
	}

	private abstract class ArrayIterator<T, V> {

		private ByteConverter<T, V> byteConverter;

		protected ArrayIterator(ByteConverter<T, V> byteConverter) {
			this.byteConverter = byteConverter;
		}

		protected abstract void remove(KeyStore store, Object object, int docId) throws IOException;
	}

	private class IntArrayIterator extends ArrayIterator<Number, Integer> {

		protected IntArrayIterator() {
			super(ByteConverter.IntegerByteConverter.INSTANCE);
		}

		@Override
		protected void remove(KeyStore store, Object object, int docId) throws IOException {
			int[] array = (int[]) object;
			for (int value : array)
				ColumnIndexKey.newInstance(colDef, value).remove(store, docId);
		}
	}

	private class DoubleArrayIterator extends ArrayIterator<Number, Double> {

		protected DoubleArrayIterator() {
			super(ByteConverter.DoubleByteConverter.INSTANCE);
		}

		@Override
		protected void remove(KeyStore store, Object object, int docId) throws IOException {
			double[] array = (double[]) object;
			for (double value : array)
				ColumnIndexKey.newInstance(colDef, value).remove(store, docId);
		}
	}

	private class LongArrayIterator extends ArrayIterator<Number, Long> {

		protected LongArrayIterator() {
			super(ByteConverter.LongByteConverter.INSTANCE);
		}

		@Override
		protected void remove(KeyStore store, Object object, int docId) throws IOException {
			long[] array = (long[]) object;
			for (long value : array)
				ColumnIndexKey.newInstance(colDef, value).remove(store, docId);
		}
	}

	private class StringArrayIterator extends ArrayIterator<String, String> {

		protected StringArrayIterator() {
			super(ByteConverter.StringByteConverter.INSTANCE);
		}

		@Override
		protected void remove(KeyStore store, Object object, int docId) throws IOException {
			String[] array = (String[]) object;
			for (String value : array)
				ColumnIndexKey.newInstance(colDef, value).remove(store, docId);
		}
	}
}