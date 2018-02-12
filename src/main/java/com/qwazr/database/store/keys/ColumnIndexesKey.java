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
package com.qwazr.database.store.keys;

import com.qwazr.database.model.InternalColumnDefinition;
import com.qwazr.database.store.KeyStore;
import com.qwazr.server.ServerException;

import javax.ws.rs.core.Response;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

public class ColumnIndexesKey extends KeysAbstract {

	final private InternalColumnDefinition colDef;
	final private ArrayIterator arrayIterator;

	public ColumnIndexesKey(InternalColumnDefinition colDef) {
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
			throw new ServerException(Response.Status.NOT_ACCEPTABLE, "Unsupported type: " + colDef.type);
		}
	}

	@Override
	final public void buildKey(final DataOutputStream output) throws IOException {
		super.buildKey(output);
		output.writeInt(colDef.columnId);
	}

	final public void remove(KeyStore store, ColumnStoreKey<?> columnStoreKey) throws IOException {
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

	private abstract class ArrayIterator {

		protected abstract void remove(KeyStore store, Object object, int docId) throws IOException;
	}

	private class IntArrayIterator extends ArrayIterator {

		@Override
		protected void remove(KeyStore store, Object object, int docId) throws IOException {
			final int[] array = (int[]) object;
			for (int value : array)
				ColumnIndexKey.newInstance(colDef, value).remove(store, docId);
		}
	}

	private class DoubleArrayIterator extends ArrayIterator {

		@Override
		final protected void remove(final KeyStore store, final Object object, final int docId) throws IOException {
			final double[] array = (double[]) object;
			for (double value : array)
				ColumnIndexKey.newInstance(colDef, value).remove(store, docId);
		}
	}

	private class LongArrayIterator extends ArrayIterator {

		@Override
		final protected void remove(final KeyStore store, final Object object, final int docId) throws IOException {
			final long[] array = (long[]) object;
			for (long value : array)
				ColumnIndexKey.newInstance(colDef, value).remove(store, docId);
		}
	}

	private class StringArrayIterator extends ArrayIterator {

		@Override
		final protected void remove(final KeyStore store, final Object object, final int docId) throws IOException {
			final String[] array = (String[]) object;
			for (String value : array)
				ColumnIndexKey.newInstance(colDef, value).remove(store, docId);
		}
	}
}