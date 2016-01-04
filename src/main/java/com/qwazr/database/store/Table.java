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
 */
package com.qwazr.database.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.store.CollectorInterface.LongCounter;
import com.qwazr.database.store.keys.*;
import com.qwazr.utils.LockUtils;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Table implements Closeable {

	public static final String ID_COLUMN_NAME = "$id$";

	private static final Logger logger = LoggerFactory.getLogger(Table.class);

	private final LockUtils.ReadWriteLock rwlColumns = new LockUtils.ReadWriteLock();

	final File directory;

	final KeyStore keyStore;

	final ColumnDefsKey columnDefsKey;

	final PrimaryIndexKey primaryIndexKey;

	private static final ExecutorService readExecutor;

	private static final ExecutorService writeExecutor;

	static {
		readExecutor = Executors.newFixedThreadPool(12);
		writeExecutor = Executors.newFixedThreadPool(4);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				readExecutor.shutdown();
				writeExecutor.shutdown();
			}
		});
	}

	public final static TypeReference<Map<String, Integer>> MapStringIntegerTypeRef = new TypeReference<Map<String, Integer>>() {
	};

	private static final ByteConverter.JsonTypeByteConverter MapStringIntegerByteConverter = new ByteConverter.JsonTypeByteConverter(
					MapStringIntegerTypeRef);

	Table(File directory) throws IOException, DatabaseException {
		this.directory = directory;

		logger.info("Load table: " + directory);
		File dbFile = new File(directory, "storedb");
		keyStore = new KeyStore(dbFile);
		columnDefsKey = new ColumnDefsKey();
		primaryIndexKey = new PrimaryIndexKey();
	}

	public void commit() throws IOException {
		keyStore.commit();
	}

	@Override
	public void close() throws IOException {
		keyStore.close();
		Tables.close(directory);
	}

	public Map<String, ColumnDefinition> getColumns() throws IOException {
		rwlColumns.r.lock();
		try {
			Map<String, ColumnDefinition.Internal> map = columnDefsKey.getColumns(keyStore);
			Map<String, ColumnDefinition> res = new LinkedHashMap<String, ColumnDefinition>();
			map.forEach((s, columnInternalDefinition) -> res.put(s, new ColumnDefinition(columnInternalDefinition)));
			return res;
		} finally {
			rwlColumns.r.unlock();
		}
	}

	public void addColumn(String columnName, ColumnDefinition columnDefinition) throws DatabaseException, IOException {
		rwlColumns.w.lock();
		try {
			// Check if the column already exists
			Map<String, ColumnDefinition.Internal> columns = columnDefsKey.getColumns(keyStore);
			if (columns != null) {
				ColumnDefinition.Internal colDef = columns.get(columnName);
				if (colDef != null)
					throw new DatabaseException("Cannot add an already existing column: " + columnName);
			}

			// Find the next available column id
			BitSet bitset = new BitSet();
			if (columns != null)
				columns.forEach((s, columnInternalDefinition) -> bitset.set(columnInternalDefinition.column_id));
			int columnd_id = bitset.nextClearBit(0);

			// Write the new column
			new ColumnDefKey(columnName)
							.setValue(keyStore, new ColumnDefinition.Internal(columnDefinition, columnd_id));
			keyStore.commit();
		} finally {
			rwlColumns.w.unlock();
		}
	}

	public void removeColumn(String columnName) throws IOException, DatabaseException {
		rwlColumns.w.lock();
		try {
			// Check if the column exists
			ColumnDefKey columnDefKey = new ColumnDefKey(columnName);
			ColumnDefinition.Internal colDef = columnDefKey.getValue(keyStore);
			if (colDef == null)
				throw new DatabaseException("Cannot delete an unknown column: " + columnName);
			// Delete stored values
			new ColumnStoresKey(colDef.column_id).deleteAll(keyStore);
			// Delete any index
			new ColumnIndexesKey(colDef).deleteAll(keyStore);
			// Remove the column definition
			columnDefKey.deleteValue(keyStore);
			keyStore.commit();
		} finally {
			rwlColumns.w.unlock();
		}
	}

	private void deleteRow(final int docId) throws IOException, DatabaseException {
		rwlColumns.r.lock();
		try {
			for (ColumnDefinition.Internal colDef : columnDefsKey.getColumns(keyStore).values()) {
				ColumnStoreKey<?> columnStoreKey = ColumnStoreKey.newInstance(colDef, docId);
				if (colDef.mode == ColumnDefinition.Mode.INDEXED)
					new ColumnIndexesKey(colDef).remove(keyStore, columnStoreKey);
				columnStoreKey.deleteValue(keyStore);
			}
		} finally {
			rwlColumns.r.unlock();
		}
	}

	public LinkedHashMap<String, Object> getRow(String key, Set<String> columnNames)
					throws IOException, DatabaseException {
		if (key == null)
			return null;
		Integer docId = new PrimaryIdsKey(key).getValue(keyStore);
		if (docId == null)
			return null;
		LinkedHashMap<String, Object> row = new LinkedHashMap<String, Object>();
		rwlColumns.r.lock();
		try {
			Map<String, ColumnDefinition.Internal> columns = columnDefsKey.getColumns(keyStore);
			for (String columnName : columnNames) {
				ColumnDefinition.Internal colDef = columns.get(columnName);
				if (colDef == null)
					throw new DatabaseException("Unknown column: " + columnName);
				row.put(columnName, ColumnStoreKey.newInstance(colDef, docId).getValue(keyStore));
			}
			return row;
		} finally {
			rwlColumns.r.unlock();
		}
	}

	public boolean deleteRow(String key) throws IOException, DatabaseException {
		if (key == null)
			return false;
		PrimaryIdsKey primaryIdsKey = new PrimaryIdsKey(key);
		Integer docId = primaryIdsKey.getValue(keyStore);
		if (docId == null)
			return false;
		deleteRow(docId);
		primaryIndexKey.remove(keyStore, docId);
		primaryIdsKey.deleteValue(keyStore);
		keyStore.commit();
		return true;
	}

	private boolean upsertRowNoCommit(String key, Map<String, Object> row) throws IOException, DatabaseException {
		if (row == null)
			return false;
		// Check if the primary key is present
		if (key == null) {
			Object o = row.get(ID_COLUMN_NAME);
			if (o != null)
				key = o.toString();
			if (key == null)
				throw new DatabaseException("The primary key is missing (" + ID_COLUMN_NAME + ")");
		}
		final boolean isNew;
		PrimaryIdsKey primaryIdsKey = new PrimaryIdsKey(key);
		Integer docId = primaryIdsKey.getValue(keyStore);
		if (docId == null) {
			isNew = true;
			docId = primaryIndexKey.nextDocId(keyStore, key);
		} else
			isNew = false;
		rwlColumns.r.lock();
		try {
			Map<String, ColumnDefinition.Internal> columns = columnDefsKey.getColumns(keyStore);
			for (Map.Entry<String, Object> entry : row.entrySet()) {
				String colName = entry.getKey();
				if (ID_COLUMN_NAME.equals(colName))
					continue;
				ColumnDefinition.Internal colDef = columns.get(colName);
				if (colDef == null)
					throw new DatabaseException("Unknown column: " + colName);
				Object valueObject = entry.getValue();
				if (colDef.mode == ColumnDefinition.Mode.INDEXED)
					new ColumnIndexesKey(colDef).select(keyStore, valueObject, docId);
				ColumnStoreKey.newInstance(colDef, docId).setObjectValue(keyStore, valueObject);
			}
			docId = null;
			return true;
		} finally {
			rwlColumns.r.unlock();
			// Rollback
			if (docId != null && isNew)
				primaryIndexKey.remove(keyStore, docId);
		}
	}

	public boolean upsertRow(String key, Map<String, Object> row) throws IOException, DatabaseException {
		boolean res = upsertRowNoCommit(key, row);
		keyStore.commit();
		return res;
	}

	public int upsertRows(Collection<Map<String, Object>> rows) throws IOException, DatabaseException {
		int count = 0;
		for (Map<String, Object> row : rows)
			if (upsertRowNoCommit(null, row))
				count++;
		keyStore.commit();
		return count;
	}

	public int getSize() throws IOException {
		return primaryIndexKey.getValue(keyStore).getCardinality();
	}

	private Map<String, ColumnDefinition.Internal> getColumns(Set<String> columnNames)
					throws DatabaseException, IOException {
		if (columnNames == null || columnNames.isEmpty())
			return Collections.emptyMap();
		Map<String, ColumnDefinition.Internal> columnDefs = columnDefsKey.getColumns(keyStore);
		Map<String, ColumnDefinition.Internal> columns = new LinkedHashMap<>();
		for (String columnName : columnNames) {
			ColumnDefinition.Internal columnDef = columnDefs.get(columnName);
			if (columnDef == null)
				throw new DatabaseException("Column not found: " + columnName);
			columns.put(columnName, columnDef);
		}
		return columns;
	}

	private LinkedHashMap<String, Object> getRowByIdNoLock(Integer docId,
					Map<String, ColumnDefinition.Internal> columns) throws DatabaseException, IOException {
		if (docId == null)
			return null;
		LinkedHashMap<String, Object> row = new LinkedHashMap<String, Object>();
		for (Map.Entry<String, ColumnDefinition.Internal> entry : columns.entrySet())
			row.put(entry.getKey(), ColumnStoreKey.newInstance(entry.getValue(), docId).getValue(keyStore));
		return row;
	}

	public void getRows(RoaringBitmap bitmap, Set<String> columnNames, long start, long rows,
					List<LinkedHashMap<String, Object>> results) throws IOException, DatabaseException {
		if (bitmap == null || bitmap.isEmpty())
			return;
		rwlColumns.r.lock();
		try {
			Map<String, ColumnDefinition.Internal> columns = getColumns(columnNames);
			IntIterator docIterator = bitmap.getIntIterator();
			while (start-- > 0 && docIterator.hasNext())
				docIterator.next();

			while (rows-- > 0 && docIterator.hasNext()) {
				LinkedHashMap<String, Object> row = getRowByIdNoLock(docIterator.next(), columns);
				if (row != null)
					results.add(row);
			}
		} finally {
			rwlColumns.r.unlock();
		}
	}

	public void getRows(Set<String> keys, Set<String> columnNames, List<LinkedHashMap<String, Object>> results)
					throws IOException, DatabaseException {
		if (keys == null || keys.isEmpty())
			return;
		ArrayList<Integer> ids = new ArrayList<Integer>(keys.size());
		PrimaryIdsKey.fillExistingIds(keyStore, keys, ids);
		if (ids == null || ids.isEmpty())
			return;
		rwlColumns.r.lock();
		try {
			Map<String, ColumnDefinition.Internal> columns = getColumns(columnNames);
			for (Integer id : ids) {
				LinkedHashMap<String, Object> row = getRowByIdNoLock(id, columns);
				// Id can be null if the document did not exists
				if (row != null)
					results.add(row);
			}
		} finally {
			rwlColumns.r.unlock();
		}
	}

	private ColumnDefinition.Internal getIndexedColumn(String columnName) throws DatabaseException, IOException {
		ColumnDefinition.Internal columnDef = new ColumnDefKey(columnName).getValue(keyStore);
		if (columnDef == null)
			throw new DatabaseException("Column not found: " + columnName);
		if (columnDef.mode != ColumnDefinition.Mode.INDEXED)
			throw new DatabaseException("The column is not indexed: " + columnName);
		return columnDef;
	}

	public QueryContext getNewQueryContext() throws IOException {
		return new QueryContext(keyStore, columnDefsKey.getColumns(keyStore));
	}

	public QueryResult query(Query query, Map<String, Map<String, LongCounter>> facets)
					throws DatabaseException, IOException {
		rwlColumns.r.lock();
		try {

			// long lastTime = System.currentTimeMillis();
			QueryContext context = getNewQueryContext();

			// First we search for the document using Bitset
			RoaringBitmap finalBitmap;
			if (query != null) {
				finalBitmap = query.execute(context, readExecutor);
				primaryIndexKey.remove(keyStore, finalBitmap);
			} else
				finalBitmap = primaryIndexKey.getValue(keyStore);
			if (finalBitmap.isEmpty())
				return new QueryResult(context, finalBitmap);

			// long newTime = System.currentTimeMillis();
			// System.out.println("Bitmap : " + (newTime - lastTime));
			// lastTime = newTime;

			// Build the collector chain
			CollectorInterface collector = CollectorInterface.build();

			// Collector chain for facets
			final Map<String, Map<Object, LongCounter>> facetsMap;
			if (facets != null) {
				facetsMap = new HashMap<String, Map<Object, LongCounter>>();
				for (Map.Entry<String, Map<String, LongCounter>> entry : facets.entrySet()) {
					String facetField = entry.getKey();
					Map<Object, LongCounter> facetMap = new HashMap<Object, LongCounter>();
					facetsMap.put(facetField, facetMap);
					collector = context.newFacetCollector(collector, facetField, facetMap);
				}
			} else
				facetsMap = null;

			// Collect the data
			collector.collect(finalBitmap);

			// newTime = System.currentTimeMillis();
			// System.out.println("Collect : " + (newTime - lastTime));
			// lastTime = newTime;

			return new QueryResult(context, finalBitmap);
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new DatabaseException(e);
		} finally {
			rwlColumns.r.unlock();
		}
	}

}
