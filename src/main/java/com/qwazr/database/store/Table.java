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

import com.fasterxml.jackson.core.type.TypeReference;
import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.TableDefinition;
import com.qwazr.database.store.CollectorInterface.LongCounter;
import com.qwazr.utils.LockUtils;
import com.qwazr.utils.threads.ThreadUtils;
import com.qwazr.utils.threads.ThreadUtils.ProcedureExceptionCatcher;
import org.apache.commons.collections4.trie.PatriciaTrie;
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
import java.util.concurrent.atomic.AtomicBoolean;

public class Table implements Closeable {

	public static final String ID_COLUMN_NAME = "_id";

	private static final Logger logger = LoggerFactory.getLogger(Table.class);

	private final static LockUtils.ReadWriteLock rwlTables = new LockUtils.ReadWriteLock();

	private final static Map<File, Table> tables = new HashMap<File, Table>();

	private final LockUtils.ReadWriteLock rwlColumns = new LockUtils.ReadWriteLock();

	private final Map<String, ColumnInterface<?>> columns = new HashMap<String, ColumnInterface<?>>();

	final File directory;

	final StoreImpl store;

	private final UniqueKey<String> primaryKey;

	final UniqueKey<String> indexedStringDictionary;

	final UniqueKey<Double> indexedDoubleDictionary;

	final UniqueKey<Long> indexedLongDictionary;

	final StoreMapInterface<String, Integer> storedPrimaryKeyMap;

	final PatriciaTrie<Integer> memoryPrimaryKeyTermMap;

	final StoreMapInterface<String, Integer> storedStringKeyMap;

	final PatriciaTrie<Integer> memoryStringKeyTermMap;

	final StoreMapInterface<Double, Integer> storedDoubleKeyMap;

	final PatriciaTrie<Integer> memoryDoubleKeyTermMap;

	final StoreMapInterface<Long, Integer> storedLongKeyMap;

	final PatriciaTrie<Integer> memoryLongKeyTermMap;

	final StoreMapInterface<String, Integer> storedKeyHigherMap;

	final StoreMapInterface<String, RoaringBitmap> storedKeyDeletedSetMap;

	final StoreMapInterface<Integer, String> storedInvertedStringDictionaryMap;

	final StoreMapInterface<Integer, Double> storedInvertedDoubleDictionaryMap;

	final StoreMapInterface<Integer, Long> storedInvertedLongDictionaryMap;

	private final StoreMapInterface<String, ColumnDefinition> storedColumnDefinitionMap;

	private final StoreMapInterface<String, Integer> storedColumnIdMap;

	private final SequenceInterface<Integer> columnIdSequence;

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

	private static final ByteConverter.JsonByteConverter<ColumnDefinition> ColumnDefinitionByteConverter =
			new ByteConverter.JsonByteConverter<ColumnDefinition>(ColumnDefinition.class);

	public final static TypeReference<Map<String, Integer>> MapStringIntegerTypeRef =
			new TypeReference<Map<String, Integer>>() {
			};


	private static final ByteConverter.JsonTypeByteConverter MapStringIntegerByteConverter =
			new ByteConverter.JsonTypeByteConverter(MapStringIntegerTypeRef);

	private Table(File directory) throws IOException, DatabaseException {
		this.directory = directory;

		logger.info("Load GraphDB (MapDB): " + directory);

		File dbFile = new File(directory, "storedb");
		store = new StoreImpl(dbFile);

		// Index structures
		storedKeyDeletedSetMap =
				store.getMap("uniqueKeyDeletedSet", ByteConverter.StringByteConverter.INSTANCE,
						ByteConverter.RoaringBitmapConverter);
		storedPrimaryKeyMap = store.getMap("storedPrimaryKeyMap", ByteConverter.StringByteConverter.INSTANCE,
				ByteConverter.IntegerByteConverter.INSTANCE);
		storedKeyHigherMap = store.getMap("storedKeyHigherMap", ByteConverter.StringByteConverter.INSTANCE,
				ByteConverter.IntegerByteConverter.INSTANCE);
		storedStringKeyMap = store.getMap("storedStringKeyMap", ByteConverter.StringByteConverter.INSTANCE,
				ByteConverter.IntegerByteConverter.INSTANCE);
		storedLongKeyMap = store.getMap("storedLongKeyMap", ByteConverter.LongByteConverter.INSTANCE,
				ByteConverter.IntegerByteConverter.INSTANCE);
		storedDoubleKeyMap = store.getMap("storedDoubleKeyMap", ByteConverter.DoubleByteConverter.INSTANCE,
				ByteConverter.IntegerByteConverter.INSTANCE);

		// Index memory structure
		memoryPrimaryKeyTermMap = new PatriciaTrie<Integer>();
		memoryStringKeyTermMap = new PatriciaTrie<Integer>();
		memoryDoubleKeyTermMap = new PatriciaTrie<Integer>();
		memoryLongKeyTermMap = new PatriciaTrie<Integer>();

		storedInvertedStringDictionaryMap = store
				.getMap("invertedStringDirectionary", ByteConverter.IntegerByteConverter.INSTANCE,
						ByteConverter.StringByteConverter.INSTANCE);
		storedInvertedDoubleDictionaryMap = store
				.getMap("invertedDoubleDirectionary", ByteConverter.IntegerByteConverter.INSTANCE,
						ByteConverter.DoubleByteConverter.INSTANCE);
		storedInvertedLongDictionaryMap = store
				.getMap("invertedLongDirectionary", ByteConverter.IntegerByteConverter.INSTANCE,
						ByteConverter.LongByteConverter.INSTANCE);

		indexedStringDictionary = UniqueKey.newStringKey(this, "dict.string");
		indexedDoubleDictionary = UniqueKey.newDoubleKey(this, "dict.double");
		indexedLongDictionary = UniqueKey.newLongKey(this, "dict.long");
		primaryKey = UniqueKey.newPrimaryKey(this, "dict.pkey");


		// Columns structures
		storedColumnIdMap = store.getMap("storedColumnIdMap", ByteConverter.StringByteConverter.INSTANCE,
				ByteConverter.IntegerByteConverter.INSTANCE);
		storedColumnDefinitionMap = store.getMap("storedColumnMap", ByteConverter.StringByteConverter.INSTANCE,
				ColumnDefinitionByteConverter);
		columnIdSequence = store.getSequence("columnIdSequence", Integer.class);
		for (Map.Entry<String, ColumnDefinition> entry : storedColumnDefinitionMap)
			loadOrCreateColumnNoLock(entry.getKey(), entry.getValue());

	}

	public static Table getInstance(File directory, boolean createIfNotExist) throws IOException, DatabaseException {
		rwlTables.r.lock();
		try {
			Table table = tables.get(directory);
			if (table != null)
				return table;
		} finally {
			rwlTables.r.unlock();
		}
		if (!createIfNotExist)
			return null;
		rwlTables.w.lock();
		try {
			Table table = tables.get(directory);
			if (table != null)
				return table;
			table = new Table(directory);
			tables.put(directory, table);
			return table;
		} finally {
			rwlTables.w.unlock();
		}
	}

	public void commit() throws IOException {
		store.commit();
	}

	@Override
	public void close() throws IOException {
		store.close();
		rwlTables.r.unlock();
		try {
			Table table = tables.get(directory);
			if (table == null)
				return;
		} finally {
			rwlTables.r.unlock();
		}
		rwlTables.w.lock();
		try {
			Table table = tables.get(directory);
			if (table == null)
				return;
			tables.remove(directory);
		} finally {
			rwlTables.w.unlock();
		}
	}

	private class LoadOrCreateColumnThread extends ProcedureExceptionCatcher {

		private final String columnName;
		private final ColumnDefinition columnDefinition;
		private final Set<String> existingColumns;

		private LoadOrCreateColumnThread(String columnName, ColumnDefinition columnDefinition,
										 Set<String> existingColumns) {
			this.columnName = columnName;
			this.columnDefinition = columnDefinition;
			this.existingColumns = existingColumns;
		}

		@Override
		public void execute() throws Exception {
			loadOrCreateColumnNoLock(columnName, columnDefinition);
			if (existingColumns != null) {
				synchronized (existingColumns) {
					existingColumns.remove(columnName);
				}
			}
		}
	}

	private void loadOrCreateColumnNoLock(String columnName, ColumnDefinition columnDefinition)
			throws IOException, DatabaseException {
		if (columns.containsKey(columnName))
			return;
		Integer columnId = storedColumnIdMap.get(columnName);
		if (columnId == null) {
			columnId = columnIdSequence.incrementAndGet();
			storedColumnIdMap.put(columnName, columnId);
		}
		ColumnInterface<?> columnInterface;
		AtomicBoolean wasExisting = new AtomicBoolean(false);
		switch (columnDefinition.mode) {
			case INDEXED:
				columnInterface = IndexedColumn.newInstance(this, columnName, columnId, columnDefinition.type);
				break;
			default:
				columnInterface = StoredColumn.newInstance(store, columnName, columnId, columnDefinition.type);
				break;
		}
		storedColumnDefinitionMap.put(columnName, columnDefinition);
		columns.put(columnName, columnInterface);
	}

	public TableDefinition getTableDefinition() {
		Map<String, ColumnDefinition> columnsDef = new LinkedHashMap<String, ColumnDefinition>();
		rwlColumns.r.lock();
		try {
			for (Map.Entry<String, ColumnDefinition> entry : storedColumnDefinitionMap)
				columnsDef.put(entry.getKey(), entry.getValue());
		} finally {
			rwlColumns.r.unlock();
		}
		return new TableDefinition(columnsDef);
	}

	public void setColumns(Map<String, ColumnDefinition> columnDefinitions)
			throws Exception {

		Set<String> columnLeft = new HashSet<String>(columns.keySet());

		rwlColumns.w.lock();
		try {
			List<LoadOrCreateColumnThread> threads = new ArrayList<LoadOrCreateColumnThread>(
					columnDefinitions.size());
			for (Map.Entry<String, ColumnDefinition> entry : columnDefinitions.entrySet())
				threads.add(new LoadOrCreateColumnThread(entry.getKey(), entry.getValue(),
						columnLeft));
			ThreadUtils.invokeAndJoin(writeExecutor, threads);
			store.commit();
		} finally {
			rwlColumns.w.unlock();
		}

		for (String columnName : columnLeft)
			removeColumn(columnName);

	}

	public void removeColumn(String columnName) throws IOException {
		rwlColumns.w.lock();
		try {
			//TODO wrong collection name
			store.deleteCollection(columnName);
			ColumnInterface<?> column = columns.get(columnName);
			if (column != null) {
				column.delete();
				columns.remove(columnName);
			}
			store.commit();
		} finally {
			rwlColumns.w.unlock();
		}
	}

	private void deleteRow(final Integer id) throws IOException {
		rwlColumns.r.lock();
		try {
			for (ColumnInterface<?> column : columns.values())
				column.deleteRow(id);
		} finally {
			rwlColumns.r.unlock();
		}
	}

	public ColumnInterface<?> getColumn(String column) throws IOException {
		rwlColumns.r.lock();
		try {
			ColumnInterface<?> columnInterface = columns.get(column);
			if (columnInterface != null)
				return columnInterface;
			throw new IOException("Column not found: " + column);
		} finally {
			rwlColumns.r.unlock();
		}
	}

	public UniqueKey<String> getPrimaryKeyIndex() {
		return primaryKey;
	}

	private ColumnInterface<?> getColumnNoLock(String columnName) throws DatabaseException {
		ColumnInterface<?> column = columns.get(columnName);
		if (column == null)
			throw new DatabaseException("Column not found: "
					+ columnName);
		return column;
	}

	public LinkedHashMap<String, Object> getRow(String key, Set<String> columnNames)
			throws IOException, DatabaseException {
		if (key == null)
			return null;
		Integer id = primaryKey.getExistingId(key);
		if (id == null)
			return null;
		LinkedHashMap<String, Object> row = new LinkedHashMap<String, Object>();
		rwlColumns.r.lock();
		try {
			for (String columnName : columnNames) {
				ColumnInterface<?> column = getColumnNoLock(columnName);
				row.put(columnName, column.getValues(id));
			}
			return row;
		} finally {
			rwlColumns.r.unlock();
		}
	}

	public boolean deleteRow(String key) throws IOException {
		if (key == null)
			return false;
		Integer id = primaryKey.getExistingId(key);
		if (id == null)
			return false;
		deleteRow(id);
		primaryKey.deleteKey(key);
		store.commit();
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
				throw new DatabaseException(
						"The primary key is missing (" + ID_COLUMN_NAME + ")");
		}
		AtomicBoolean isNew = new AtomicBoolean();
		Integer docId = primaryKey.getIdOrNew(key, isNew);
		if (docId == null)
			return false;
		rwlColumns.r.lock();
		try {
			for (Map.Entry<String, Object> entry : row.entrySet()) {
				String colName = entry.getKey();
				if (ID_COLUMN_NAME.equals(colName))
					continue;
				ColumnInterface<?> column = getColumnNoLock(colName);
				Object object = entry.getValue();
				if (object instanceof Collection)
					column.setValues(docId, (Collection) object);
				else
					column.setValue(docId, object);
			}
			key = null;
			return true;
		} finally {
			rwlColumns.r.unlock();
			if (key != null && isNew.get())
				primaryKey.deleteKey(key);
		}
	}

	public boolean upsertRow(String key, Map<String, Object> row) throws IOException, DatabaseException {
		boolean res = upsertRowNoCommit(key, row);
		store.commit();
		return res;
	}

	public int upsertRows(Collection<Map<String, Object>> rows) throws IOException, DatabaseException {
		int count = 0;
		for (Map<String, Object> row : rows)
			if (upsertRowNoCommit(null, row))
				count++;
		store.commit();
		return count;
	}

	public int getSize() {
		return primaryKey.size();
	}

	private class CompiledColumn {

		private final String name;

		private final ColumnInterface column;

		private CompiledColumn(String name) throws DatabaseException {
			this.column = columns.get(name);
			if (column == null)
				throw new DatabaseException("Column not found: " + name);
			this.name = name;
		}
	}

	static private final CompiledColumn[] EMPTY_COLUMNS = new CompiledColumn[0];

	private CompiledColumn[] getColumns(Set<String> columnNames) throws DatabaseException {
		if (columnNames == null || columnNames.isEmpty())
			return EMPTY_COLUMNS;
		CompiledColumn[] columns = new CompiledColumn[columnNames.size()];
		int i = 0;
		for (String columnName : columnNames)
			columns[i++] = new CompiledColumn(columnName);
		return columns;
	}

	private LinkedHashMap<String, Object> getRowByIdNoLock(Integer id, CompiledColumn[] columns)
			throws DatabaseException, IOException {
		if (id == null)
			return null;
		LinkedHashMap<String, Object> row = new LinkedHashMap<String, Object>();
		for (CompiledColumn column : columns)
			row.put(column.name, column.column.getValues(id));
		return row;
	}

	public void getRows(RoaringBitmap bitmap, Set<String> columnNames, long start,
						long rows, List<LinkedHashMap<String, Object>> results)
			throws IOException, DatabaseException {
		if (bitmap == null || bitmap.isEmpty())
			return;
		rwlColumns.r.lock();
		try {
			CompiledColumn[] columns = getColumns(columnNames);
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
		primaryKey.fillExistingIds(keys, ids);
		if (ids == null || ids.isEmpty())
			return;
		rwlColumns.r.lock();
		try {
			CompiledColumn[] columns = getColumns(columnNames);
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

	@SuppressWarnings("unchecked")
	<T> IndexedColumn<T> getIndexedColumn(String columnName) throws DatabaseException {
		ColumnInterface<?> column = columns.get(columnName);
		if (column == null)
			throw new DatabaseException("Column not found: " + columnName);
		if (!(column instanceof IndexedColumn))
			throw new DatabaseException("The column is not indexed: "
					+ columnName);
		return ((IndexedColumn<T>) column);
	}

	public RoaringBitmap query(Query query,
							   Map<String, Map<String, LongCounter>> facets) throws DatabaseException, IOException {
		rwlColumns.r.lock();
		try {

			// long lastTime = System.currentTimeMillis();

			// First we search for the document using Bitset
			RoaringBitmap finalBitmap;
			if (query != null) {
				finalBitmap = query.execute(this, readExecutor);
				primaryKey.removeDeleted(finalBitmap);
			} else
				finalBitmap = primaryKey.getActiveSet();
			if (finalBitmap.isEmpty())
				return finalBitmap;

			// long newTime = System.currentTimeMillis();
			// System.out.println("Bitmap : " + (newTime - lastTime));
			// lastTime = newTime;

			// Build the collector chain
			CollectorInterface collector = CollectorInterface.build();

			// Collector chain for facets
			final Map<String, Map<Integer, LongCounter>> termIdFacetsMap;
			if (facets != null) {
				termIdFacetsMap = new HashMap<String, Map<Integer, LongCounter>>();
				for (Map.Entry<String, Map<String, LongCounter>> entry : facets
						.entrySet()) {
					String facetField = entry.getKey();
					Map<Integer, LongCounter> termIdFacetMap = new HashMap<Integer, LongCounter>();
					termIdFacetsMap.put(facetField, termIdFacetMap);
					collector = getIndexedColumn(facetField).newFacetCollector(
							collector, termIdFacetMap);
				}
			} else
				termIdFacetsMap = null;

			// newTime = System.currentTimeMillis();
			// System.out.println("Collect chain : " + (newTime - lastTime));
			// lastTime = newTime;

			// Extract the data
			collector.collect(finalBitmap);

			// newTime = System.currentTimeMillis();
			// System.out.println("Collect : " + (newTime - lastTime));
			// lastTime = newTime;

			// Resolve facets termIds
			if (facets != null) {
				List<ProcedureExceptionCatcher> threads = new ArrayList<ProcedureExceptionCatcher>(
						facets.size());
				for (Map.Entry<String, Map<String, LongCounter>> entry : facets
						.entrySet()) {
					String facetField = entry.getKey();
					threads.add(new ProcedureExceptionCatcher() {
						@Override
						public void execute() throws Exception {
							getIndexedColumn(facetField).resolveFacetsIds(
									termIdFacetsMap.get(facetField),
									entry.getValue());
						}
					});
				}
				ThreadUtils.invokeAndJoin(readExecutor, threads);
			}

			// newTime = System.currentTimeMillis();
			// System.out.println("Facet : " + (newTime - lastTime));
			// lastTime = newTime;

			return finalBitmap;
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new DatabaseException(e);
		} finally {
			rwlColumns.r.unlock();
		}
	}
}
