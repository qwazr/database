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
import com.qwazr.utils.FunctionUtils;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;

public class IndexKey extends KeyAbstract<RoaringBitmap, RoaringBitmap> {

	final static ByteConverter.SerializableByteConverter<RoaringBitmap> roaringBitmapConverter =
			new ByteConverter.SerializableByteConverter<>();

	public IndexKey(KeyEnum keyType) {
		super(keyType, roaringBitmapConverter);
	}

	/**
	 * Remove a document from the index
	 *
	 * @param store the store
	 * @param docId the internal id of the document
	 * @throws IOException
	 */
	public void remove(final KeyStore store, final int docId) throws IOException {
		RoaringBitmap bitmap = getValue(store);
		if (bitmap == null)
			return;
		bitmap.remove(docId);
		if (bitmap.isEmpty())
			deleteValue(store);
		else
			setValue(store, bitmap);
	}

	/**
	 * Add a document to the index
	 *
	 * @param store the store
	 * @param docId the internal id of the document
	 * @throws IOException
	 */
	final public void select(final KeyStore store, final int docId) throws IOException {
		RoaringBitmap bitmap = getValue(store);
		if (bitmap == null)
			bitmap = new RoaringBitmap();
		bitmap.add(docId);
		setValue(store, bitmap);
	}

	/**
	 * Look for the next available document ID.
	 *
	 * @param store the store
	 * @return an available document ID
	 * @throws IOException
	 */
	final protected Integer nextDocId(final KeyStore store) throws IOException {
		final RoaringBitmap bitmap = getValue(store);
		if (bitmap == null || bitmap.isEmpty())
			return 0;
		final IntIterator reverseIterator = bitmap.getReverseIntIterator();
		final int nexHigherId = reverseIterator.next() + 1;

		final RoaringBitmap inverseBitmap = bitmap.clone();
		inverseBitmap.flip(0L, nexHigherId);
		final IntIterator inverseIterator = inverseBitmap.getIntIterator();
		if (inverseIterator.hasNext())
			return inverseIterator.next();
		return nexHigherId;
	}

	final public void range(final KeyStore store, int start, int rows, final FunctionUtils.IntConsumerEx docIdConsumer)
			throws IOException {
		final RoaringBitmap bitmap = getValue(store);
		if (bitmap == null || bitmap.isEmpty())
			return;
		final IntIterator iterator = bitmap.getIntIterator();
		while (iterator.hasNext() && start-- > 0)
			iterator.next();
		while (iterator.hasNext() && rows-- > 0)
			docIdConsumer.accept(iterator.next());
	}

}