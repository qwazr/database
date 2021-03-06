/*
 * Copyright 2015-2018 Emmanuel Keller / QWAZR
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
 */
package com.qwazr.database;

import com.qwazr.database.model.*;
import com.qwazr.database.store.*;
import com.qwazr.server.ServerException;
import com.qwazr.utils.ExceptionUtils;
import com.qwazr.utils.FileUtils;
import com.qwazr.utils.concurrent.ReadWriteLock;
import org.roaringbitmap.RoaringBitmap;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class TableManager {

    private final ReadWriteLock rwl = ReadWriteLock.stamped();

    private final ExecutorService executorService;

    private final Path tablesDirectory;

    private final TableServiceInterface service;

    public TableManager(final ExecutorService executorService, final Path tablesDirectory) throws ServerException {
        this.executorService = executorService;
        this.tablesDirectory = tablesDirectory;
        service = new TableServiceImpl(this);
    }

    public static Path checkTablesDirectory(final Path dataDirectory) throws IOException {
        final Path tablesDirectory = dataDirectory.resolve(TableServiceInterface.SERVICE_NAME);
        if (!Files.exists(tablesDirectory))
            Files.createDirectory(tablesDirectory);
        return tablesDirectory;
    }

    public TableServiceInterface getService() {
        return service;
    }

    private Table getTable(final String tableName) {
        final Path tableDirectory = tablesDirectory.resolve(tableName);
        if (!Files.exists(tableDirectory))
            throw new ServerException(Response.Status.NOT_FOUND, "Table not found: " + tableName);
        return Tables.getInstance(executorService, tableDirectory.toFile(), null);
    }

    public void createTable(final String tableName, final KeyStore.Impl storeImpl) throws IOException {
        rwl.writeEx(() -> {
            final Path tableDirectory = tablesDirectory.resolve(tableName);
            if (Files.exists(tableDirectory))
                throw new ServerException(Response.Status.CONFLICT, "The table already exists: " + tableName);
            Files.createDirectory(tableDirectory);
            if (!Files.exists(tableDirectory))
                throw new ServerException(Response.Status.INTERNAL_SERVER_ERROR,
                        "The directory cannot be created: " + tableDirectory.toAbsolutePath().toString());
            Tables.getInstance(executorService, tableDirectory.toFile(), storeImpl);
        });
    }

    public SortedSet<String> getNameSet() {
        return rwl.read(() -> {
            final TreeSet<String> names = new TreeSet<>();
            try (final Stream<Path> stream = Files.list(tablesDirectory)) {
                stream.filter(p -> Files.isDirectory(p))
                        .filter(p -> ExceptionUtils.bypass(() -> !Files.isHidden(p)))
                        .forEach(p -> names.add(p.getFileName().toString()));
            }
            return names;
        });
    }

    public TableStatus getStatus(final String tableName) throws IOException {
        return rwl.readEx(() -> {
            final Table table = getTable(tableName);
            final TableDefinition definition = new TableDefinition(table.getImplementation(), table.getColumns());
            return new TableStatus(definition, table.getSize());
        });
    }

    public Map<String, ColumnDefinition> getColumns(final String tableName) throws IOException {
        return rwl.readEx(() -> getTable(tableName).getColumns());
    }

    public void setColumn(final String tableName, final String columnName, final ColumnDefinition columnDefinition)
            throws IOException {
        rwl.writeEx(() -> {
            getTable(tableName).setColumn(columnName, columnDefinition);
        });
    }

    public void removeColumn(final String tableName, final String columnName) throws IOException {
        rwl.writeEx(() -> {
            getTable(tableName).removeColumn(columnName);
        });
    }

    public List<Object> getColumnTerms(final String tableName, final String columnName, final Integer start,
                                       final Integer rows) throws IOException {
        return rwl.readEx(() -> getTable(tableName).getColumnTerms(columnName, start == null ? 0 : start,
                rows == null ? 10 : rows));
    }

    public List<String> getColumnTermKeys(final String tableName, final String columnName, final String term,
                                          final Integer start, final Integer rows) throws IOException {
        return rwl.readEx(() -> getTable(tableName).getColumnTermKeys(columnName, term, start == null ? 0 : start,
                rows == null ? 10 : rows));
    }

    public void deleteTable(final String tableName) throws IOException {
        rwl.writeEx(() -> {
            final Path tableDirectory = tablesDirectory.resolve(tableName);
            if (!Files.exists(tableDirectory))
                throw new ServerException(Response.Status.NOT_FOUND, "Table not found: " + tableName);
            Tables.delete(tableDirectory.toFile());
            FileUtils.deleteDirectory(tableDirectory);
        });
    }

    public void upsertRow(final String tableName, final String row_id, final Map<String, Object> nodeMap)
            throws IOException {
        rwl.readEx(() -> getTable(tableName).upsertRow(row_id, nodeMap));
    }

    public int upsertRows(final String tableName, final List<Map<String, Object>> rows) throws IOException {
        return rwl.readEx(() -> getTable(tableName).upsertRows(rows));
    }

    public LinkedHashMap<String, Object> getRow(final String tableName, final String key, final Set<String> columns)
            throws IOException {
        return rwl.readEx(() -> {
            final Table table = getTable(tableName);
            final LinkedHashMap<String, Object> row = table.getRow(key, columns);
            if (row == null)
                throw new ServerException(Response.Status.NOT_FOUND, "Row not found: " + key);
            return row;
        });
    }

    public List<Map<String, Object>> getRows(final String tableName, final Set<String> columns, final Set<String> keys)
            throws IOException {
        return rwl.readEx(() -> {
            final Table table = getTable(tableName);
            final List<Map<String, Object>> rows = new ArrayList<>();
            table.getRows(keys, columns, rows);
            return rows;
        });
    }

    public List<String> getPrimaryKeys(final String tableName, final Integer start, final Integer rows)
            throws IOException {
        return rwl.readEx(
                () -> getTable(tableName).getPrimaryKeys(start == null ? 0 : start, rows == null ? 10 : rows));
    }

    public boolean deleteRow(final String tableName, final String key) throws IOException {
        return rwl.readEx(() -> getTable(tableName).deleteRow(key));
    }

    public TableRequestResult query(final String tableName, final TableRequest request) throws IOException {
        return rwl.readEx(() -> {

            final long start = request.start == null ? 0 : request.start;
            final long rows = request.rows == null ? Long.MAX_VALUE : request.rows;

            final Table table = getTable(tableName);

            final Map<String, Map<String, CollectorInterface.LongCounter>> counters;
            if (request.counters != null && !request.counters.isEmpty()) {
                counters = new LinkedHashMap<>();
                request.counters.forEach(col -> counters.put(col, new HashMap<>()));
            } else
                counters = null;

            final Query query = request.query == null ? null : Query.prepare(request.query, null);

            final RoaringBitmap docBitset = table.query(query, counters).finalBitmap;

            if (docBitset == null || docBitset.isEmpty())
                return new TableRequestResult(0L, Collections.emptyList(), Collections.emptyMap());

            final TableRequestResult.Builder builder =
                    new TableRequestResult.Builder((long) docBitset.getCardinality());

            final List<Map<String, Object>> resultRows = new ArrayList<>();
            table.getRows(docBitset, request.columns, start, rows, resultRows);
            builder.rows(resultRows);

            if (counters == null)
                return builder.build();

            final Map<String, Map<String, Long>> resultCounters = new LinkedHashMap<>();
            counters.forEach((countersEntryKey, countersEntry) -> {
                final LinkedHashMap<String, Long> counter = new LinkedHashMap<>();
                countersEntry.forEach((key, counterEntry) -> counter.put(key, counterEntry.count));
                resultCounters.put(countersEntryKey, counter);
            });
            builder.counters(resultCounters);

            return builder.build();
        });
    }

}
