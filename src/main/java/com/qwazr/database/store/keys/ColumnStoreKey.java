/**
 * Copyright 2015 Emmanuel Keller / QWAZR
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
import com.qwazr.database.store.ValueConsumer;
import com.qwazr.utils.ArrayUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;

final public class ColumnStoreKey<T> extends KeyAbstract<T> {

    final ColumnDefinition.Internal columnDef;
    final int docId;

    protected ColumnStoreKey(ColumnDefinition.Internal columnDef, int docId, ByteConverter<T> byteConverter) {
	super(KeyEnum.COLUMN_STORE, byteConverter);
	this.docId = docId;
	this.columnDef = columnDef;
    }

    @Override
    final public void buildKey(final DataOutputStream output) throws IOException {
	super.buildKey(output);
	output.writeInt(columnDef.column_id);
	output.writeInt(docId);
    }

    final public void forEach(final KeyStore store, final ValueConsumer consumer) throws IOException {
	byteConverter.forEach(getValue(store), consumer);
    }

    final public void forFirst(final KeyStore store, final ValueConsumer consumer) throws IOException {
	byteConverter.forFirst(getValue(store), consumer);
    }

    final public static ColumnStoreKey<?> newInstance(ColumnDefinition.Internal colDef, int docId)
		    throws DatabaseException {
	ByteConverter<?> byteConverter;
	switch (colDef.type) {
	case DOUBLE:
	    return new ColumnStoreKey<double[]>(colDef, docId, ByteConverter.DoubleArrayByteConverter.INSTANCE);
	case INTEGER:
	    return new ColumnStoreKey<int[]>(colDef, docId, ByteConverter.IntArrayByteConverter.INSTANCE);
	case LONG:
	    return new ColumnStoreKey<long[]>(colDef, docId, ByteConverter.LongArrayByteConverter.INSTANCE);
	case STRING:
	    return new ColumnStoreKey<String[]>(colDef, docId, ByteConverter.StringArrayByteConverter.INSTANCE);
	}
	throw new DatabaseException("unknown type: " + colDef.type);
    }

    final public void setObjectValue(KeyStore store, Object value) throws IOException, DatabaseException {
	if (value == null)
	    return;
	if (value instanceof Collection<?>)
	    setValue(store, (T) collectionToArray((Collection<?>) value));
	else if (value.getClass().isArray())
	    setValue(store, (T) value);
	else
	    setValue(store, (T) objetToArray(value));
    }

    private Object collectionToArray(Collection<?> collection) throws DatabaseException {
	switch (columnDef.type) {
	case DOUBLE:
	    return ArrayUtils.toPrimitiveDouble((Collection<Double>) collection);
	case INTEGER:
	    return ArrayUtils.toPrimitiveInt((Collection<Integer>) collection);
	case LONG:
	    return ArrayUtils.toPrimitiveLong((Collection<Long>) collection);
	case STRING:
	    return collection.toArray(new String[collection.size()]);
	}
	throw new DatabaseException("unknown type: " + columnDef.type);
    }

    private Object objetToArray(Object object) throws DatabaseException {
	switch (columnDef.type) {
	case DOUBLE:
	    return new double[] { (Double) object };
	case INTEGER:
	    return new int[] { (Integer) object };
	case LONG:
	    return new long[] { (Long) object };
	case STRING:
	    return new String[] { (String) object };
	}
	throw new DatabaseException("unknown type: " + columnDef.type);
    }
}
