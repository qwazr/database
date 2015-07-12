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
package com.qwazr.database;

import com.fasterxml.jackson.core.type.TypeReference;
import com.qwazr.database.CollectorInterface.LongCounter;
import com.qwazr.database.UniqueKey.UniqueDoubleKey;
import com.qwazr.database.UniqueKey.UniqueStringKey;
import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.TableDefinition;
import com.qwazr.database.store.*;
import com.qwazr.utils.LockUtils;
import com.qwazr.utils.threads.ThreadUtils;
import com.qwazr.utils.threads.ThreadUtils.FunctionExceptionCatcher;
import com.qwazr.utils.threads.ThreadUtils.ProcedureExceptionCatcher;
import org.apache.commons.io.FileUtils;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
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

	private final File directory;

	private final StoreInterface storeDb;

	private final UniqueStringKey primaryKey;

	private final UniqueStringKey indexedStringDictionary;

	private final UniqueDoubleKey indexedDoubleDictionary;

	private final StoreMapInterface<Integer, String> storedInvertedStringDictionaryMap;

	private final StoreMapInterface<Integer, Double> storedInvertedDoubleDictionaryMap;

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

	private Table(File directory) throws IOException {
		this.directory = directory;

		logger.info("Load GraphDB (MapDB): " + directory);

		// Load the storage database
		FunctionExceptionCatcher<Object> storeDbLoader = new FunctionExceptionCatcher<Object>() {
			@Override
			public StoreInterface execute() throws Exception {
				File dbFile = new File(directory, "storedb");
				return new StoreImpl(dbFile);
			}
		};

		// Load the primary key
		FunctionExceptionCatcher<Object> primaryKeyLoader = new FunctionExceptionCatcher<Object>() {
			@Override
			public UniqueStringKey execute() throws Exception {
				return new UniqueStringKey(directory, "pkey");
			}
		};

		// Load the indexed dictionary
		FunctionExceptionCatcher<Object> indexedStringDictionaryLoader = new FunctionExceptionCatcher<Object>() {
			@Override
			public UniqueStringKey execute() throws Exception {
				return new UniqueStringKey(directory, "dict");
			}
		};

		// Load the indexed dictionary
		FunctionExceptionCatcher<Object> indexedDoubleDictionaryLoader = new FunctionExceptionCatcher<Object>() {
			@Override
			public UniqueDoubleKey execute() throws Exception {
				return new UniqueDoubleKey(directory, "dict.double");
			}
		};

		try {
			ThreadUtils.invokeAndJoin(writeExecutor, Arrays.asList(
					storeDbLoader, primaryKeyLoader,
					indexedStringDictionaryLoader,
					indexedDoubleDictionaryLoader));
		} catch (Exception e) {
			throw new IOException(e);
		}

		storeDb = (StoreInterface) storeDbLoader.getResult();
		primaryKey = (UniqueStringKey) primaryKeyLoader.getResult();
		indexedStringDictionary = (UniqueStringKey) indexedStringDictionaryLoader
				.getResult();
		storedInvertedStringDictionaryMap = storeDb
				.getMap("invertedDirectionary", ByteConverter.IntegerByteConverter.INSTANCE,
						ByteConverter.StringByteConverter.INSTANCE);
		indexedDoubleDictionary = (UniqueDoubleKey) indexedDoubleDictionaryLoader
				.getResult();
		storedInvertedDoubleDictionaryMap = storeDb
				.getMap("invertedDoubleDirectionary", ByteConverter.IntegerByteConverter.INSTANCE,
						ByteConverter.DoubleByteConverter.INSTANCE);
		storedColumnIdMap = storeDb.getMap("storedColumnIdMap", ByteConverter.StringByteConverter.INSTANCE,
				ByteConverter.IntegerByteConverter.INSTANCE);
		storedColumnDefinitionMap = storeDb.getMap("storedColumnMap", ByteConverter.StringByteConverter.INSTANCE,
				ColumnDefinitionByteConverter);
		columnIdSequence = storeDb.getSequence("columnIdSequence", Integer.class);
		for (Map.Entry<String, ColumnDefinition> entry : storedColumnDefinitionMap)
			loadOrCreateColumnNoLock(entry.getKey(), entry.getValue());

	}

	public static Table getInstance(File directory, boolean createIfNotExist) throws IOException {
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

	public static void deleteTable(File directory) throws IOException {
		rwlTables.r.lock();
		try {
			Table table = tables.get(directory);
			if (table == null)
				throw new FileNotFoundException("Table not found: " + directory.getAbsolutePath());
		} finally {
			rwlTables.r.unlock();
		}
		rwlTables.w.lock();
		try {
			Table table = tables.get(directory);
			if (table == null)
				return;
			tables.remove(directory);
			table.delete();
		} finally {
			rwlTables.w.unlock();
		}

	}

	private void commit() throws IOException {

		logger.info("Commit " + directory);

		List<ProcedureExceptionCatcher> threads = new ArrayList<ProcedureExceptionCatcher>();

		threads.add(new ProcedureExceptionCatcher() {
			@Override
			public void execute() throws Exception {
				primaryKey.commit();
			}
		});

		threads.add(new ProcedureExceptionCatcher() {
			@Override
			public void execute() throws Exception {
				indexedStringDictionary.commit();
			}
		});

		threads.add(new ProcedureExceptionCatcher() {
			@Override
			public void execute() throws Exception {
				indexedDoubleDictionary.commit();
			}
		});

		threads.add(new ProcedureExceptionCatcher() {
			@Override
			public void execute() throws Exception {
				storeDb.commit();
			}
		});

		for (ColumnInterface<?> field : columns.values()) {
			threads.add(new ProcedureExceptionCatcher() {
				@Override
				public void execute() throws Exception {
					field.commit();
				}
			});
		}

		try {
			ThreadUtils.invokeAndJoin(writeExecutor, threads);
		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new IOException(e);
		}

	}

	public void rollback() {
		logger.info("Rollback " + directory);
		storeDb.rollback();
	}

	@Override
	public void close() throws IOException {
		storeDb.close();
	}

	private void delete() throws IOException {
		logger.info("Delete " + directory);
		close();
		FileUtils.deleteDirectory(directory);
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

	private void loadOrCreateColumnNoLock(String columnName, ColumnDefinition columnDefinition) throws IOException {
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
				switch (columnDefinition.type) {
					default:
					case STRING:
						columnInterface = new IndexedColumn.IndexedStringColumn(columnName, columnId,
								directory, indexedStringDictionary,
								storedInvertedStringDictionaryMap, wasExisting);
						break;
					case DOUBLE:
						columnInterface = new IndexedColumn.IndexedDoubleColumn(columnName, columnId,
								directory, indexedDoubleDictionary,
								storedInvertedDoubleDictionaryMap, wasExisting);
						break;
				}
				break;
			default:
				switch (columnDefinition.type) {
					default:
					case STRING:
						columnInterface = new StoredColumn.StoredStringColumn(columnName, columnId,
								storeDb, wasExisting);
						break;
					case DOUBLE:
						columnInterface = new StoredColumn.StoredDoubleColumn(columnName, columnId,
								storeDb, wasExisting);
						break;
				}
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
		} finally {
			rwlColumns.w.unlock();
		}

		for (String columnName : columnLeft)
			removeColumn(columnName);

	}

	public void removeColumn(String columnName) throws IOException {
		rwlColumns.w.lock();
		try {
			storeDb.delete(columnName);
			ColumnInterface<?> column = columns.get(columnName);
			if (column != null) {
				column.delete();
				columns.remove(columnName);
			}
		} finally {
			rwlColumns.w.unlock();
		}
	}

	void deleteRow(final Integer id) throws IOException {
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

	public UniqueStringKey getPrimaryKeyIndex() {
		return primaryKey;
	}

	private ColumnInterface<?> getColumnNoLock(String columnName) throws DatabaseException {
		ColumnInterface<?> column = columns.get(columnName);
		if (column == null)
			throw new DatabaseException("Column not found: "
					+ columnName);
		return column;
	}

	public LinkedHashMap<String, Object> getRow(String key, Set<String> columns) throws IOException, DatabaseException {
		if (key == null)
			return null;
		Integer id = primaryKey.getExistingId(key);
		if (id == null)
			return null;
		LinkedHashMap<String, Object> row = new LinkedHashMap<String, Object>();
		rwlColumns.r.lock();
		try {
			for (String column : columns) {
				ColumnInterface<?> field = getColumnNoLock(column);
				row.put(column, field.getValues(id));
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
				throw new IllegalArgumentException(
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
		commit();
		return res;
	}

	public int upsertRows(Collection<Map<String, Object>> rows) throws IOException, DatabaseException {
		int count = 0;
		for (Map<String, Object> row : rows)
			if (upsertRowNoCommit(null, row))
				count++;
		commit();
		return count;
	}

	public int getSize() {
		return primaryKey.size();
	}

	public List<LinkedHashMap<String, Object>> getRows(Set<String> keys,
													   Set<String> columnNames) throws IOException {
		if (keys == null || keys.isEmpty())
			return null;
		ArrayList<Integer> ids = new ArrayList<Integer>(keys.size());
		primaryKey.fillExistingIds(keys, ids);
		if (ids == null || ids.isEmpty())
			return null;
		List<LinkedHashMap<String, Object>> rows = new ArrayList<LinkedHashMap<String, Object>>(
				ids.size());
		rwlColumns.r.lock();
		try {
			for (Integer id : ids) {
				LinkedHashMap<String, Object> row = null;
				// Id can be null if the document did not exists
				if (id != null) {
					row = new LinkedHashMap<String, Object>();
					for (String columnName : columnNames) {
						ColumnInterface<?> column = columns.get(columnName);
						if (column == null)
							throw new IllegalArgumentException(
									"Column not found: " + columnName);
						row.put(columnName, column.getValues(id));
					}
				}
				rows.add(row);
			}
			return rows;
		} finally {
			rwlColumns.r.unlock();
		}
	}

	@SuppressWarnings("unchecked")
	<T> IndexedColumn<T> getIndexedColumn(String columnName) {
		ColumnInterface<?> column = columns.get(columnName);
		if (column == null)
			throw new IllegalArgumentException("Column not found: " + columnName);
		if (!(column instanceof IndexedColumn))
			throw new IllegalArgumentException("The column is not indexed: "
					+ columnName);
		return ((IndexedColumn<T>) column);
	}

	public RoaringBitmap query(Query query,
							   Map<String, Map<String, LongCounter>> facets) {
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
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			rwlColumns.r.unlock();
		}
	}
}
