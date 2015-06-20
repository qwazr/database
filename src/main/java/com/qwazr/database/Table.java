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

import com.qwazr.database.CollectorInterface.LongCounter;
import com.qwazr.database.FieldInterface.FieldDefinition;
import com.qwazr.database.IndexedField.IndexedDoubleField;
import com.qwazr.database.IndexedField.IndexedStringField;
import com.qwazr.database.StoredField.StoredDoubleField;
import com.qwazr.database.StoredField.StoredStringField;
import com.qwazr.database.UniqueKey.UniqueDoubleKey;
import com.qwazr.database.UniqueKey.UniqueStringKey;
import com.qwazr.database.storeDb.*;
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
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class Table implements Closeable {

	private static final Logger logger = LoggerFactory.getLogger(Table.class);

	private final static LockUtils.ReadWriteLock rwlTables = new LockUtils.ReadWriteLock();

	private final static Map<File, Table> tables = new HashMap<File, Table>();

	private final LockUtils.ReadWriteLock rwlFields = new LockUtils.ReadWriteLock();

	private final Map<String, FieldInterface<?>> fields = new HashMap<String, FieldInterface<?>>();

	private final File directory;

	private final StoreInterface storeDb;

	private final UniqueStringKey primaryKey;

	private final UniqueStringKey indexedStringDictionary;

	private final UniqueDoubleKey indexedDoubleDictionary;

	private final StoreMap<Integer, String> storedInvertedStringDictionaryMap;

	private final StoreMap<Integer, Double> storedInvertedDoubleDictionaryMap;

	private final StoreMap<String, Long> storedFieldIdMap;

	private final LongSequence fieldIdSequence;

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

	private Table(File directory) throws IOException {
		this.directory = directory;

		logger.info("Load GraphDB (MapDB): " + directory);

		// Load the storage database
		FunctionExceptionCatcher<Object> storeDbLoader = new FunctionExceptionCatcher<Object>() {
			@Override
			public StoreInterface execute() throws Exception {
				File dbFile = new File(directory, "storedb");
				return new LevelDBImpl(dbFile);
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
				.getMap("invertedDirectionary", ByteConverter.IntegerByteConverter.INSTANCE, ByteConverter.StringByteConverter.INSTANCE);
		indexedDoubleDictionary = (UniqueDoubleKey) indexedDoubleDictionaryLoader
				.getResult();
		storedInvertedDoubleDictionaryMap = storeDb
				.getMap("invertedDoubleDirectionary", ByteConverter.IntegerByteConverter.INSTANCE, ByteConverter.DoubleByteConverter.INSTANCE);
		storedFieldIdMap = storeDb.getMap("storedFieldIdMap", ByteConverter.StringByteConverter.INSTANCE, ByteConverter.LongByteConverter.INSTANCE);
		fieldIdSequence = storeDb.getLongSequence("fieldIdSequence");
	}

	public static Table getInstance(File directory) throws IOException {
		rwlTables.r.lock();
		try {
			Table table = tables.get(directory);
			if (table != null)
				return table;
		} finally {
			rwlTables.r.unlock();
		}
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
			table.delete();
		} finally {
			rwlTables.w.unlock();
		}

	}

	public void commit() throws IOException {

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

		for (FieldInterface<?> field : fields.values()) {
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

	public void collectExistingFields(final Collection<String> existingFields) {
		rwlFields.r.lock();
		try {
			existingFields.addAll(fields.keySet());
		} finally {
			rwlFields.r.unlock();
		}
	}

	private class LoadOrCreateFieldThread extends ProcedureExceptionCatcher {

		private final FieldDefinition fieldDefinition;
		private final AtomicBoolean needSave;
		private final Set<String> existingFields;

		private LoadOrCreateFieldThread(FieldDefinition fieldDefinition,
										AtomicBoolean needSave, Set<String> existingFields) {
			this.fieldDefinition = fieldDefinition;
			this.needSave = needSave;
			this.existingFields = existingFields;
		}

		@Override
		public void execute() throws Exception {
			loadOrCreateFieldNoLock(fieldDefinition, needSave);
			if (existingFields != null) {
				synchronized (existingFields) {
					existingFields.remove(fieldDefinition.name);
				}
			}
		}
	}

	private void loadOrCreateFieldNoLock(FieldDefinition fieldDefinition,
										 AtomicBoolean needSave) throws IOException {
		if (fields.containsKey(fieldDefinition.name))
			return;
		Long fieldId = storedFieldIdMap.get(fieldDefinition.name);
		if (fieldId == null) {
			fieldId = fieldIdSequence.incrementAndGet();
			storedFieldIdMap.put(fieldDefinition.name, fieldId);
		}
		FieldInterface<?> field;
		AtomicBoolean wasExisting = new AtomicBoolean(false);
		switch (fieldDefinition.mode) {
			case INDEXED:
				switch (fieldDefinition.type) {
					default:
					case STRING:
						field = new IndexedStringField(fieldDefinition.name, fieldId,
								directory, indexedStringDictionary,
								storedInvertedStringDictionaryMap, wasExisting);
						break;
					case DOUBLE:
						field = new IndexedDoubleField(fieldDefinition.name, fieldId,
								directory, indexedDoubleDictionary,
								storedInvertedDoubleDictionaryMap, wasExisting);
						break;
				}
				break;
			default:
				switch (fieldDefinition.type) {
					default:
					case STRING:
						field = new StoredStringField(fieldDefinition.name, fieldId,
								storeDb, wasExisting);
						break;
					case DOUBLE:
						field = new StoredDoubleField(fieldDefinition.name, fieldId,
								storeDb, wasExisting);
						break;
				}
				break;
		}
		if (!wasExisting.get())
			needSave.set(true);
		fields.put(fieldDefinition.name, field);
	}

	public void setFields(Collection<FieldDefinition> fieldDefinitions,
						  Set<String> existingFields, AtomicBoolean needCommit)
			throws Exception {
		if (fieldDefinitions == null || fieldDefinitions.isEmpty())
			return;
		rwlFields.w.lock();
		try {
			List<LoadOrCreateFieldThread> threads = new ArrayList<LoadOrCreateFieldThread>(
					fieldDefinitions.size());
			for (FieldDefinition fieldDefinition : fieldDefinitions)
				threads.add(new LoadOrCreateFieldThread(fieldDefinition,
						needCommit, existingFields));
			ThreadUtils.invokeAndJoin(writeExecutor, threads);
		} finally {
			rwlFields.w.unlock();
		}
	}

	public void removeField(String fieldName) throws IOException {
		rwlFields.w.lock();
		try {
			storeDb.delete(fieldName);
			FieldInterface<?> field = fields.get(fieldName);
			if (field != null) {
				field.delete();
				fields.remove(fieldName);
			}
		} finally {
			rwlFields.w.unlock();
		}
	}

	void deleteDocument(final Integer id) throws IOException {
		rwlFields.r.lock();
		try {
			for (FieldInterface<?> field : fields.values())
				field.deleteDocument(id);
		} finally {
			rwlFields.r.unlock();
		}
	}

	public FieldInterface<?> getField(String field) throws IOException {
		rwlFields.r.lock();
		try {
			FieldInterface<?> fieldInterface = fields.get(field);
			if (fieldInterface != null)
				return fieldInterface;
			throw new IOException("Field not found: " + field);
		} finally {
			rwlFields.r.unlock();
		}
	}

	public UniqueStringKey getPrimaryKeyIndex() {
		return primaryKey;
	}

	public Map<String, List<?>> getDocument(String key,
											Collection<String> returnedFields) throws IOException {
		if (key == null)
			return null;
		Integer id = primaryKey.getExistingId(key);
		if (id == null)
			return null;
		Map<String, List<?>> document = new HashMap<String, List<?>>();
		rwlFields.r.lock();
		try {
			for (String returnedField : returnedFields) {
				FieldInterface<?> field = fields.get(returnedField);
				if (field == null)
					throw new IllegalArgumentException("Field not found: "
							+ returnedField);
				document.put(returnedField, field.getValues(id));
			}
			return document;
		} finally {
			rwlFields.r.unlock();
		}
	}

	public boolean deleteDocument(String key) throws IOException {
		if (key == null)
			return false;
		Integer id = primaryKey.getExistingId(key);
		if (id == null)
			return false;
		deleteDocument(id);
		primaryKey.deleteKey(key);
		return true;
	}

	public int getSize() {
		return primaryKey.size();
	}

	public List<Map<String, List<?>>> getDocuments(Collection<String> keys,
												   Collection<String> returnedFields) throws IOException {
		if (keys == null || keys.isEmpty())
			return null;
		ArrayList<Integer> ids = new ArrayList<Integer>(keys.size());
		primaryKey.fillExistingIds(keys, ids);
		if (ids == null || ids.isEmpty())
			return null;
		List<Map<String, List<?>>> documents = new ArrayList<Map<String, List<?>>>(
				ids.size());
		rwlFields.r.lock();
		try {
			for (Integer id : ids) {
				Map<String, List<?>> document = null;
				// Id can be null if the document did not exists
				if (id != null) {
					document = new HashMap<String, List<?>>();
					for (String returnedField : returnedFields) {
						FieldInterface<?> field = fields.get(returnedField);
						if (field == null)
							throw new IllegalArgumentException(
									"Field not found: " + returnedField);
						document.put(returnedField, field.getValues(id));
					}
				}
				documents.add(document);
			}
			return documents;
		} finally {
			rwlFields.r.unlock();
		}
	}

	@SuppressWarnings("unchecked")
	<T> IndexedField<T> getIndexedField(String fieldName) {
		FieldInterface<?> field = fields.get(fieldName);
		if (field == null)
			throw new IllegalArgumentException("Field not found: " + fieldName);
		if (!(field instanceof IndexedField))
			throw new IllegalArgumentException("The field is not indexed: "
					+ fieldName);
		return ((IndexedField<T>) field);
	}

	public RoaringBitmap query(Query query,
							   Map<String, Map<String, LongCounter>> facets) {
		rwlFields.r.lock();
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
					collector = getIndexedField(facetField).newFacetCollector(
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
							getIndexedField(facetField).resolveFacetsIds(
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
			rwlFields.r.unlock();
		}
	}
}
