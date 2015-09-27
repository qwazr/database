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

import java.io.IOException;
import java.io.ObjectOutputStream;

public class ColumnIndexKey<T> extends IndexKey {

    final private int columnId;
    final private T value;
    final private ByteConverter valueByteConverter;

    protected ColumnIndexKey(int columnId, T value, ByteConverter valueByteConverter) {
	super(KeyEnum.COLUMN_INDEX);
	this.columnId = columnId;
	this.value = value;
	this.valueByteConverter = valueByteConverter;
    }

    @Override
    final public void buildKey(final ObjectOutputStream os) throws IOException {
	super.buildKey(os);
	os.writeInt(columnId);
	os.write(valueByteConverter.toBytes(value));
    }

    public static ColumnIndexKey<?> newInstance(ColumnDefinition.Internal colDef, Object value)
		    throws DatabaseException {
	ByteConverter<?> byteConverter;
	switch (colDef.type) {
	case DOUBLE:
	    return new ColumnIndexKey<Object>(colDef.column_id, value, ByteConverter.DoubleArrayByteConverter.INSTANCE);
	case INTEGER:
	    return new ColumnIndexKey<Object>(colDef.column_id, value, ByteConverter.IntArrayByteConverter.INSTANCE);
	case LONG:
	    return new ColumnIndexKey<Object>(colDef.column_id, value, ByteConverter.LongArrayByteConverter.INSTANCE);
	case STRING:
	    return new ColumnIndexKey<Object>(colDef.column_id, value, ByteConverter.StringArrayByteConverter.INSTANCE);
	}
	throw new DatabaseException("unknown type: " + colDef.type);
    }
}