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
import com.qwazr.database.store.KeyStore;
import com.qwazr.utils.CharsetUtils;
import org.iq80.leveldb.DBIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

final public class ColumnDefsKey extends KeyAbstract<Map<String, ColumnDefinition.Internal>> {

    public ColumnDefsKey() {
	super(KeyEnum.COLUMN_DEF, null);
    }

    public Map<String, ColumnDefinition.Internal> getColumns(final KeyStore store) throws IOException {
	final Map<String, ColumnDefinition.Internal> map = new LinkedHashMap<>();
	final byte[] myKey = getCachedKey();
	final ByteBuffer bb = ByteBuffer.wrap(myKey);
	final DBIterator iterator = store.iterator();
	iterator.seek(myKey);
	while (iterator.hasNext()) {
	    Map.Entry<byte[], byte[]> entry = iterator.next();
	    byte[] key = entry.getKey();
	    if (key.length < myKey.length)
		break;
	    if (!bb.equals(ByteBuffer.wrap(key, 0, myKey.length)))
		break;
	    String fieldName = CharsetUtils.decodeUtf8(ByteBuffer.wrap(key, myKey.length, key.length - myKey.length));
	    ColumnDefinition.Internal colDef = ColumnDefKey.columnInternalDefinitionByteConverter
			    .toValue(entry.getValue());
	    map.put(fieldName, colDef);
	}
	return map;
    }

}
