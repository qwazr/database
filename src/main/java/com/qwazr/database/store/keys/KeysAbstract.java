/**
 * Copyright 2015-2016 Emmanuel Keller / QWAZR
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

import com.qwazr.database.store.KeyStore;
import org.iq80.leveldb.DBIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public abstract class KeysAbstract<T> extends KeyAbstract<T> {

    public KeysAbstract(KeyEnum keyType) {
	super(keyType, null);
    }

    final public int deleteAll(KeyStore store) throws IOException {
	byte[] myKey = getCachedKey();
	ByteBuffer bb = ByteBuffer.wrap(myKey);
	DBIterator iterator = store.iterator();
	iterator.seek(myKey);
	int count = 0;
	while (iterator.hasNext()) {
	    Map.Entry<byte[], byte[]> entry = iterator.next();
	    byte[] key = entry.getKey();
	    if (key.length < myKey.length)
		break;
	    if (!bb.equals(ByteBuffer.wrap(key, 0, myKey.length)))
		break;
	    store.delete(key);
	    count++;
	}
	return count;
    }
}