/**
 * Copyright 2015 OpenSearchServer Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwarz.database;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qwarz.database.CollectorInterface.LongCounter;
import com.qwarz.database.FieldInterface.FieldDefinition;
import com.qwazr.utils.LockUtils;
import com.qwazr.utils.threads.ThreadUtils;
import com.qwazr.utils.threads.ThreadUtils.FunctionExceptionCatcher;
import com.qwazr.utils.threads.ThreadUtils.ProcedureExceptionCatcher;

public class Database implements Closeable {

	private static final Logger logger = LoggerFactory
			.getLogger(Database.class);

	private final static LockUtils.ReadWriteLock rwlBases = new LockUtils.ReadWriteLock();

	private final static Map<File, Database> bases = new HashMap<File, Database>();

	private final LockUtils.ReadWriteLock rwlFields = new LockUtils.ReadWriteLock();

	private final Map<String, FieldInterface> fields = new HashMap<String, FieldInterface>();

	private final File directory;

	private final DB storeDb;

	private final UniqueKey primaryKey;

	private final UniqueKey indexedDictionary;

	private final Map<Integer, String> storedInvertedDictionaryMap;

	private final Map<String, Long> storedFieldIdMap;

	private final Atomic.Long fieldIdSequence;

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

	private Database(File directory) throws IOException {
		this.directory = directory;

		logger.info("Load GraphDB (MapDB): " + directory);

		// Load the storage database
		FunctionExceptionCatcher<Object> storeDbLoader = new FunctionExceptionCatcher<Object>() {
			@Override
			public DB execute() throws Exception {
				return DBMaker.newFileDB(new File(directory, "store.mapdb"))
						.cacheLRUEnable().closeOnJvmShutdown()
						.compressionEnable().make();
			}
		};

		// Load the primary key
		FunctionExceptionCatcher<Object> primaryKeyLoader = new FunctionExceptionCatcher<Object>() {
			@Override
			public UniqueKey execute() throws Exception {
				return new UniqueKey(directory, "pkey");
			}
		};

		// Load the indexed dictionary
		FunctionExceptionCatcher<Object> indexedDictionaryLoader = new FunctionExceptionCatcher<Object>() {
			@Override
			public UniqueKey execute() throws Exception {
				return new UniqueKey(directory, "dict");
			}
		};

		try {
			ThreadUtils.invokeAndJoin(writeExecutor, Arrays.asList(
					storeDbLoader, primaryKeyLoader, indexedDictionaryLoader));
		} catch (Exception e) {
			throw new IOException(e);
		}

		storeDb = (DB) storeDbLoader.getResult();
		primaryKey = (UniqueKey) primaryKeyLoader.getResult();
		indexedDictionary = (UniqueKey) indexedDictionaryLoader.getResult();
		storedInvertedDictionaryMap = storeDb
				.getTreeMap("invertedDirectionary");
		storedFieldIdMap = storeDb.getTreeMap("storedFieldIdMap");
		Atomic.Long longSequence = storeDb.getAtomicLong("fieldIdSequence");
		if (longSequence == null)
			longSequence = storeDb.createAtomicLong("fieldIdSequence", 0);
		fieldIdSequence = longSequence;
	}

	public static Database getInstance(File directory) throws IOException {
		rwlBases.r.lock();
		try {
			Database base = bases.get(directory);
			if (base != null)
				return base;
		} finally {
			rwlBases.r.unlock();
		}
		rwlBases.w.lock();
		try {
			Database base = bases.get(directory);
			if (base != null)
				return base;
			base = new Database(directory);
			bases.put(directory, base);
			return base;
		} finally {
			rwlBases.w.unlock();
		}
	}

	public static void deleteBase(File directory) throws IOException {
		rwlBases.r.lock();
		try {
			Database base = bases.get(directory);
			if (base == null)
				return;
		} finally {
			rwlBases.r.unlock();
		}
		rwlBases.w.lock();
		try {
			Database base = bases.get(directory);
			if (base == null)
				return;
			bases.remove(directory);
			base.delete();
		} finally {
			rwlBases.w.unlock();
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
				indexedDictionary.commit();
			}
		});

		threads.add(new ProcedureExceptionCatcher() {
			@Override
			public void execute() throws Exception {
				storeDb.commit();
			}
		});

		for (FieldInterface field : fields.values()) {
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
		FieldInterface field;
		AtomicBoolean wasExisting = new AtomicBoolean(false);
		switch (fieldDefinition.type) {
		case INDEXED:
			field = new IndexedField(fieldDefinition.name, fieldId, directory,
					indexedDictionary, storedInvertedDictionaryMap, wasExisting);
			break;
		default:
			field = new StoredField(fieldDefinition.name, fieldId, storeDb,
					wasExisting);
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
			FieldInterface field = fields.get(fieldName);
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
			for (FieldInterface field : fields.values())
				field.deleteDocument(id);
		} finally {
			rwlFields.r.unlock();
		}
	}

	private FieldInterface getField(String field) throws IOException {
		FieldInterface fieldInterface = fields.get(field);
		if (fieldInterface != null)
			return fieldInterface;
		throw new IOException("Field not found: " + field);
	}

	public void setValues(Integer id, String field, Collection<String> values)
			throws IOException {
		rwlFields.r.lock();
		try {
			getField(field).setValues(id, values);
		} finally {
			rwlFields.r.unlock();
		}
	}

	public void setValue(Integer id, String field, String value)
			throws IOException {
		rwlFields.r.lock();
		try {
			getField(field).setValue(id, value);
		} finally {
			rwlFields.r.unlock();
		}
	}

	public Integer getNewPrimaryId(String id) {
		return primaryKey.getIdOrNew(id, null);
	}

	public Map<String, List<String>> getDocument(String key,
			Collection<String> returnedFields) throws IOException {
		if (key == null)
			return null;
		Integer id = primaryKey.getExistingId(key);
		if (id == null)
			return null;
		Map<String, List<String>> document = new HashMap<String, List<String>>();
		rwlFields.r.lock();
		try {
			for (String returnedField : returnedFields) {
				FieldInterface field = fields.get(returnedField);
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
		return true;
	}

	public List<Map<String, List<String>>> getDocuments(
			Collection<String> keys, Collection<String> returnedFields)
			throws IOException {
		if (keys == null || keys.isEmpty())
			return null;
		ArrayList<Integer> ids = new ArrayList<Integer>(keys.size());
		primaryKey.fillExistingIds(keys, ids);
		if (ids == null || ids.isEmpty())
			return null;
		List<Map<String, List<String>>> documents = new ArrayList<Map<String, List<String>>>(
				ids.size());
		rwlFields.r.lock();
		try {
			for (Integer id : ids) {
				Map<String, List<String>> document = null;
				// Id can be null if the document did not exists
				if (id != null) {
					document = new HashMap<String, List<String>>();
					for (String returnedField : returnedFields) {
						FieldInterface field = fields.get(returnedField);
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

	IndexedField getIndexedField(String fieldName) {
		FieldInterface field = fields.get(fieldName);
		if (field == null)
			throw new IllegalArgumentException("Field not found: " + fieldName);
		if (!(field instanceof IndexedField))
			throw new IllegalArgumentException("The field is not indexed: "
					+ fieldName);
		return ((IndexedField) field);
	}

	private RoaringBitmap findDocumentsOr(Map<String, Set<String>> orTermQuery)
			throws Exception {
		final RoaringBitmap finalBitmap = new RoaringBitmap();
		List<ProcedureExceptionCatcher> threads = new ArrayList<ProcedureExceptionCatcher>(
				orTermQuery.size());
		for (Map.Entry<String, Set<String>> entry : orTermQuery.entrySet()) {
			IndexedField field = getIndexedField(entry.getKey());

			threads.add(new ProcedureExceptionCatcher() {
				@Override
				public void execute() throws Exception {
					RoaringBitmap bitmap = field.getTermBitSetOr(entry
							.getValue());
					if (bitmap != null) {
						synchronized (finalBitmap) {
							finalBitmap.or(bitmap);
						}
					}
				}
			});
		}
		ThreadUtils.invokeAndJoin(readExecutor, threads);
		return finalBitmap;
	}

	public int findDocumentsOr(Map<String, Set<String>> orTermQuery,
			Collection<Integer> documentIds,
			Map<String, Map<String, LongCounter>> facets) {
		rwlFields.r.lock();
		try {

			// long lastTime = System.currentTimeMillis();

			// First we search for the document using Bitset
			RoaringBitmap finalBitmap = findDocumentsOr(orTermQuery);
			if (finalBitmap.isEmpty())
				return 0;

			// long newTime = System.currentTimeMillis();
			// System.out.println("Bitmap : " + (newTime - lastTime));
			// lastTime = newTime;

			// Build the collector chain
			CollectorInterface collector = CollectorInterface.build();
			if (documentIds != null)
				collector = collector.documents(documentIds);

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

			return collector.getCount();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			rwlFields.r.unlock();
		}
	}
}
