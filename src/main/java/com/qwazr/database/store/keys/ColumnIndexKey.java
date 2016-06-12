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
import com.qwazr.database.store.KeyStore;
import com.qwazr.utils.server.ServerException;

import javax.ws.rs.core.Response;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ColumnIndexKey<V> extends IndexKey {

	final private int columnId;
	final private V value;
	final private ByteConverter<V> valueByteConverter;

	private ColumnIndexKey(int columnId, Object value, ByteConverter<V> valueByteConverter) throws IOException {
		super(KeyEnum.COLUMN_INDEX);
		this.columnId = columnId;
		this.value = value == null ? null : valueByteConverter.convert(value);
		this.valueByteConverter = valueByteConverter;
	}

	@Override
	final public void buildKey(final DataOutputStream output) throws IOException {
		super.buildKey(output);
		output.writeInt(columnId);
		if (value != null)
			output.write(valueByteConverter.toBytes(value));
	}

	public List<Object> getValues(final KeyStore store, final int start, final int rows) throws IOException {
		final List<Object> values = new ArrayList<>();
		final byte[] prefixKey = getCachedKey();
		prefixedKeys(store, start, rows, (key, value) -> {
			ByteBuffer valueBytes = ByteBuffer.wrap(key, prefixKey.length, key.length - prefixKey.length);
			values.add(valueByteConverter.toValue(valueBytes));
		});
		return values;
	}

	final public static ColumnIndexKey<?> newInstance(final ColumnDefinition.Internal colDef, Object value)
			throws IOException {

		switch (colDef.type) {
		case DOUBLE:
			return new ColumnIndexKey<>(colDef.column_id, value, ByteConverter.DoubleByteConverter.INSTANCE);
		case INTEGER:
			return new ColumnIndexKey<>(colDef.column_id, value, ByteConverter.IntegerByteConverter.INSTANCE);
		case LONG:
			return new ColumnIndexKey<>(colDef.column_id, value, ByteConverter.LongByteConverter.INSTANCE);
		case STRING:
			return new ColumnIndexKey<>(colDef.column_id, null, ByteConverter.StringByteConverter.INSTANCE);
		}
		throw new ServerException(Response.Status.NOT_ACCEPTABLE, "unknown type: " + colDef.type);
	}
}