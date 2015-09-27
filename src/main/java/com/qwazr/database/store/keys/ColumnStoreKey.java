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

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collection;

final public class ColumnStoreKey<T> extends KeyAbstract<T> {

    final int columnId;
    final int docId;

    protected ColumnStoreKey(int columnId, int docId, ByteConverter<T> byteConverter) {
	super(KeyEnum.COLUMN_STORE, byteConverter);
	this.docId = docId;
	this.columnId = columnId;
    }

    @Override
    final public void buildKey(final ObjectOutputStream os) throws IOException {
	super.buildKey(os);
	os.writeInt(columnId);
	os.writeInt(docId);
    }

    final public static ColumnStoreKey<?> newInstance(ColumnDefinition.Internal colDef, int docId)
		    throws DatabaseException {
	ByteConverter<?> byteConverter;
	switch (colDef.type) {
	case DOUBLE:
	    return new ColumnStoreKey<double[]>(colDef.column_id, docId,
			    ByteConverter.DoubleArrayByteConverter.INSTANCE);
	case INTEGER:
	    return new ColumnStoreKey<int[]>(colDef.column_id, docId, ByteConverter.IntArrayByteConverter.INSTANCE);
	case LONG:
	    return new ColumnStoreKey<long[]>(colDef.column_id, docId, ByteConverter.LongArrayByteConverter.INSTANCE);
	case STRING:
	    return new ColumnStoreKey<String[]>(colDef.column_id, docId,
			    ByteConverter.StringArrayByteConverter.INSTANCE);
	}
	throw new DatabaseException("unknown type: " + colDef.type);
    }

    final public void setObjectValue(KeyStore store, Object value) throws IOException {
	if (value instanceof Collection<?>) {
	    for (Object val : (Collection<?>) value)
		setValue(store, (T) val);
	} else
	    setValue(store, (T) value);
    }
}
