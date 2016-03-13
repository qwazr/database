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

import com.qwazr.database.store.KeyStore;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

public class PrimaryIndexKey extends IndexKey {

	public PrimaryIndexKey() {
		super(KeyEnum.PRIMARY_INDEX);
	}

	final public Integer nextDocId(final KeyStore store, String key) throws IOException {
		int docId = super.nextDocId(store);
		new PrimaryIdsKey(key).setValue(store, docId);
		new PrimaryKeysKey(docId).setValue(store, key);
		return docId;
	}

	@Override
	public void remove(final KeyStore store, final int docId) throws IOException {
		PrimaryKeysKey primaryKeysKey = new PrimaryKeysKey(docId);
		String key = primaryKeysKey.getValue(store);
		if (key != null)
			new PrimaryIdsKey(key).deleteValue(store);
		new PrimaryKeysKey(docId).deleteValue(store);
		super.remove(store, docId);
	}

	public void remove(final KeyStore store, final RoaringBitmap finalBitmap) throws IOException {
		if (finalBitmap == null || finalBitmap.isEmpty())
			return;
		IntIterator intIterator = finalBitmap.getIntIterator();
		while (intIterator.hasNext()) {
			remove(store, intIterator.next());
		}
	}
}
