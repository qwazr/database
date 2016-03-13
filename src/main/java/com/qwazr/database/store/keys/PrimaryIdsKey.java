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

import com.qwazr.database.store.ByteConverter;
import com.qwazr.database.store.KeyStore;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

public class PrimaryIdsKey extends KeyAbstract<Integer> {

	private final String key;

	public PrimaryIdsKey(String key) {
		super(KeyEnum.PRIMARY_IDS, ByteConverter.IntegerByteConverter.INSTANCE);
		this.key = key;
	}

	@Override
	public void buildKey(final DataOutputStream output) throws IOException {
		super.buildKey(output);
		output.writeChars(key);
	}

	/**
	 * Resolve the keys by collecting the internal docId(s)
	 *
	 * @param store
	 * @param keys  a list of key to resolve
	 * @param ids   the collection
	 * @throws IOException
	 */
	final static public void fillExistingIds(final KeyStore store, final Set<String> keys,
			final Collection<Integer> ids) throws IOException {
		if (keys == null || keys.isEmpty())
			return;
		for (String key : keys) {
			Integer id = new PrimaryIdsKey(key).getValue(store);
			if (id != null)
				ids.add(id);
		}
	}

}
