/**
 * Copyright 2015 Emmanuel Keller / QWAZR
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.database.store;

import com.qwazr.database.store.CollectorInterface.LongCounter;
import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.utils.LockUtils;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class IndexedColumn<T> extends ColumnAbstract<T> {

	private final LockUtils.ReadWriteLock rwl = new LockUtils.ReadWriteLock();

	private final UniqueKey<T> indexedDictionary;

	private final StoreMapInterface<Integer, RoaringBitmap> docBitsetsMap;

	private final StoreMapInterface<Integer, int[]> termVectorMap;

	private final StoreMapInterface<Integer, T> storedInvertedDictionaryMap;

	protected IndexedColumn(Table table, String name, long columnId, UniqueKey<T> indexedDictionary,
							StoreMapInterface<Integer, T> storedInvertedDictionaryMap) throws IOException {
		super(name, columnId);
		this.indexedDictionary = indexedDictionary;
		this.storedInvertedDictionaryMap = storedInvertedDictionaryMap;
		this.docBitsetsMap = table.storeDb.getMap("column." + columnId + ".idx.",
				ByteConverter.IntegerByteConverter.INSTANCE,
				ByteConverter.RoaringBitmapConverter);
		this.termVectorMap =
				table.storeDb.getMap("column." + columnId + ".tv", ByteConverter.IntegerByteConverter.INSTANCE,
						ByteConverter.IntArrayByteConverter.INSTANCE);
	}

	@Override
	public void delete() {
		//TODO delete keys
	}

	private void setTermDocNoLock(Integer docId, Integer termId) throws IOException {
		RoaringBitmap docBitSet = docBitsetsMap.get(termId);
		if (docBitSet == null) {
			docBitSet = new RoaringBitmap();
		}
		if (!docBitSet.contains(docId)) {
			docBitSet.add(docId);
			docBitsetsMap.put(termId, docBitSet);
		}
	}

	private Integer getTermIdOrNew(T term) throws IOException {
		AtomicBoolean isNewTerm = new AtomicBoolean();
		Integer termId = indexedDictionary.getIdOrNew(term, isNewTerm);
		// Its a new term, we store it in the dictionary
		if (isNewTerm.get())
			storedInvertedDictionaryMap.put(termId, term);
		return termId;
	}


	private Set<Integer> getTermVectorIdSet(Integer docId) throws IOException {
		Set<Integer> idSet = new HashSet<Integer>();
		int[] idArray = termVectorMap.get(docId);
		if (idArray != null)
			for (int id : idArray)
				idSet.add(id);
		return idSet;
	}

	private void putTermVectorIdSet(Integer docId, Set<Integer> termIdSet)
			throws IOException {
		if (termIdSet.isEmpty()) {
			termVectorMap.remove(docId);
			return;
		}
		int[] idArray = new int[termIdSet.size()];
		int i = 0;
		for (Integer termId : termIdSet)
			idArray[i++] = termId;
		termVectorMap.put(docId, idArray);
	}

	private void setTerms(Integer docId, Set<Integer> newTermIdSet)
			throws IOException {
		// Get the previous set (if any)
		Set<Integer> oldTermIdSet = getTermVectorIdSet(docId);

		// Check if the old and the new id set are identical
		boolean isIdentical = oldTermIdSet != null
				&& oldTermIdSet.size() == newTermIdSet.size()
				&& oldTermIdSet.containsAll(newTermIdSet);

		if (!isIdentical)
			putTermVectorIdSet(docId, newTermIdSet);

		// Update the bitmaps
		for (Integer termId : newTermIdSet)
			setTermDocNoLock(docId, termId);
	}

	private void setTerm(Integer docId, Integer termId) throws IOException {
		// Get the previous set (if any)
		Set<Integer> termIdSet = getTermVectorIdSet(docId);
		if (termIdSet == null)
			termIdSet = new HashSet<Integer>();

		// Check if the old and the new id set are identical
		boolean isIdentical = termIdSet != null && termIdSet.size() == 1
				&& termIdSet.contains(termId);

		if (!isIdentical) {
			termIdSet.add(termId);
			putTermVectorIdSet(docId, termIdSet);
		}

		// Update the bitmap
		setTermDocNoLock(docId, termId);
	}

	@Override
	public void setValues(final Integer docId, Collection<Object> values)
			throws IOException {

		if (values == null || values.isEmpty())
			return;

		// Prepare the id of the terms
		final Set<Integer> newTermIdSet = new HashSet<Integer>();
		for (Object value : values)
			newTermIdSet.add(getTermIdOrNew(convertValue(value)));

		rwl.w.lock();
		try {
			setTerms(docId, newTermIdSet);
		} finally {
			rwl.w.unlock();
		}
	}

	@Override
	public void setValue(final Integer docId, Object value) throws IOException {
		if (value == null)
			return;
		// Get the term ID
		final Integer termId = getTermIdOrNew(convertValue(value));
		rwl.w.lock();
		try {
			setTerm(docId, termId);
		} finally {
			rwl.w.unlock();
		}
	}

	@Override
	public T getValue(final Integer docId) throws IOException {
		rwl.r.lock();
		try {
			int[] termIdArray = termVectorMap.get(docId);
			if (termIdArray == null || termIdArray.length == 0)
				return null;
			return storedInvertedDictionaryMap.get(termIdArray[0]);
		} finally {
			rwl.r.unlock();
		}
	}

	public int[] getTerms(Integer docId) throws IOException {
		rwl.r.lock();
		try {
			return termVectorMap.get(docId);
		} finally {
			rwl.r.unlock();
		}
	}

	@Override
	public List<T> getValues(Integer docId) throws IOException {
		rwl.r.lock();
		try {
			int[] termIdArray = termVectorMap.get(docId);
			if (termIdArray == null || termIdArray.length == 0)
				return null;
			List<T> list = new ArrayList<T>(termIdArray.length);
			for (int termId : termIdArray)
				list.add(storedInvertedDictionaryMap.get(termId));
			return list;
		} finally {
			rwl.r.unlock();
		}
	}

	@Override
	public void collectValues(Iterator<Integer> docIds,
							  ColumnValueCollector<T> collector) throws IOException {
		rwl.r.lock();
		try {
			Integer docId;
			while ((docId = docIds.next()) != null) {
				int[] termIdArray = termVectorMap.get(docId);
				if (termIdArray == null || termIdArray.length == 0)
					continue;
				for (int termId : termIdArray)
					collector.collect(storedInvertedDictionaryMap.get(termId));
			}
		} catch (NoSuchElementException | ArrayIndexOutOfBoundsException e) {
			// Faster use the exception than calling hasNext for each document
		} finally {
			rwl.r.unlock();
		}
	}

	private RoaringBitmap getDocBitSetNoLock(T term) throws IOException {
		Integer termId = indexedDictionary.getExistingId(term);
		if (termId == null)
			return null;
		return docBitsetsMap.get(termId);
	}

	RoaringBitmap getDocBitSet(T term) throws IOException {
		rwl.r.lock();
		try {
			return getDocBitSetNoLock(term);
		} finally {
			rwl.r.unlock();
		}
	}

	RoaringBitmap getDocBitSetOr(Set<T> terms) throws IOException {
		rwl.r.lock();
		try {
			RoaringBitmap finalBitMap = null;
			for (T term : terms) {
				RoaringBitmap bitMap = getDocBitSetNoLock(term);
				if (bitMap == null)
					continue;
				if (finalBitMap == null)
					finalBitMap = bitMap;
				else
					finalBitMap.or(bitMap);
			}
			return finalBitMap;
		} finally {
			rwl.r.unlock();
		}
	}

	RoaringBitmap getDocBitSetAnd(Set<T> terms) throws IOException {
		rwl.r.lock();
		try {
			RoaringBitmap finalBitMap = null;
			for (T term : terms) {
				RoaringBitmap bitMap = getDocBitSetNoLock(term);
				if (bitMap == null)
					continue;
				if (finalBitMap == null)
					finalBitMap = bitMap;
				else
					finalBitMap.and(bitMap);
			}
			return finalBitMap;
		} finally {
			rwl.r.unlock();
		}
	}

	@Override
	public void deleteRow(Integer docId) throws IOException {
		rwl.r.lock();
		try {
			int[] termIdArray = termVectorMap.get(docId);
			if (termIdArray == null)
				return;
			for (int termId : termIdArray) {
				RoaringBitmap bitSet = docBitsetsMap.get(termId);
				if (bitSet != null && bitSet.contains(docId)) {
					bitSet.remove(docId);
					docBitsetsMap.put(termId, bitSet);
				}
			}
			termVectorMap.remove(docId);
		} finally {
			rwl.r.unlock();
		}
	}

	public CollectorInterface newFacetCollector(CollectorInterface collector,
												Map<Integer, LongCounter> termCounter) {
		rwl.r.lock();
		try {
			return collector.facets(termVectorMap, termCounter);
		} finally {
			rwl.r.unlock();
		}
	}

	public void resolveFacetsIds(Map<Integer, LongCounter> termIdMap,
								 Map<String, LongCounter> termMap) throws IOException {
		if (termIdMap == null)
			return;
		rwl.r.lock();
		try {
			for (Map.Entry<Integer, LongCounter> entry : termIdMap.entrySet()) {
				String term = storedInvertedDictionaryMap.get(entry.getKey())
						.toString();
				termMap.put(term, entry.getValue());
			}
		} finally {
			rwl.r.unlock();
		}
	}

	private static class IndexedStringColumn extends IndexedColumn<String> {

		private IndexedStringColumn(Table table, String name, long columnId) throws IOException {
			super(table, name, columnId, table.indexedStringDictionary, table.storedInvertedStringDictionaryMap);
		}

		@Override
		final public String convertValue(final Object value) {
			if (value instanceof String)
				return (String) value;
			return value.toString();
		}
	}

	private static class IndexedDoubleColumn extends IndexedColumn<Double> {

		private IndexedDoubleColumn(Table table, String name, long columnId) throws IOException {
			super(table, name, columnId, table.indexedDoubleDictionary, table.storedInvertedDoubleDictionaryMap);
		}

		@Override
		final public Double convertValue(final Object value) {
			if (value instanceof Double)
				return (Double) value;
			return Double.valueOf(value.toString());
		}
	}

	private static class IndexedLongColumn extends IndexedColumn<Long> {

		private IndexedLongColumn(Table table, String name, long columnId) throws IOException {
			super(table, name, columnId, table.indexedLongDictionary, table.storedInvertedLongDictionaryMap);
		}

		@Override
		final public Long convertValue(final Object value) {
			if (value instanceof Long)
				return (Long) value;
			return Long.valueOf(value.toString());
		}
	}

	static IndexedColumn<?> newInstance(Table table, String columnName, Integer columnId,
										ColumnDefinition.Type columnType) throws IOException, DatabaseException {

		switch (columnType) {
			case STRING:
				return new IndexedStringColumn(table, columnName, columnId);
			case DOUBLE:
				return new IndexedDoubleColumn(table, columnName, columnId);
			case LONG:
				return new IndexedLongColumn(table, columnName, columnId);
			default:
				throw new DatabaseException("Unsupported type: " + columnType);
		}
	}
}
