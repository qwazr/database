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

import com.qwazr.database.model.InternalColumnDefinition;
import com.qwazr.database.store.KeyStore;
import com.qwazr.utils.CharsetUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

final public class ColumnDefsKey extends KeyAbstract<Map<String, InternalColumnDefinition>> {

	public ColumnDefsKey() {
		super(KeyEnum.COLUMN_DEF, null);
	}

	public Map<String, InternalColumnDefinition> getColumns(final KeyStore store) throws IOException {
		final Map<String, InternalColumnDefinition> map = new LinkedHashMap<>();
		final byte[] prefixKey = getCachedKey();
		prefixedKeys(store, 0, Integer.MAX_VALUE, (key, value) -> {
			String fieldName =
					CharsetUtils.decodeUtf8(ByteBuffer.wrap(key, prefixKey.length, key.length - prefixKey.length));
			InternalColumnDefinition colDef = ColumnDefKey.columnInternalDefinitionByteConverter.toValue(value);
			map.put(fieldName, colDef);
		});
		return map;
	}
}
