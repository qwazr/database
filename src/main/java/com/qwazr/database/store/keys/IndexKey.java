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

import com.qwazr.database.store.ByteConverter;
import com.qwazr.database.store.KeyStore;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

public class IndexKey extends KeyAbstract<RoaringBitmap> {

    final static ByteConverter.SerializableByteConverter<RoaringBitmap> roaringBitmapConverter = new ByteConverter.SerializableByteConverter<>();

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
	    return;
	bitmap.select(docId);
	setValue(store, bitmap);
    }

    final protected Integer nextDocId(final KeyStore store) throws IOException {
	RoaringBitmap bitmap = getValue(store);
	if (bitmap == null)
	    return 0;
	if (bitmap.isEmpty())
	    return 0;
	IntIterator reverseIterator = bitmap.getReverseIntIterator();
	int highterId = reverseIterator.next();
	bitmap = bitmap.clone();
	bitmap.flip(0, Integer.MAX_VALUE);
	IntIterator iterator = bitmap.getIntIterator();
	if (iterator.hasNext())
	    return iterator.next();
	return highterId + 1;
    }

}