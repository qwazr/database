/*
 * Copyright 2016-2017 Emmanuel Keller / QWAZR
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
package com.qwazr.database.annotations;

import com.qwazr.binder.FieldMapWrapper;
import com.qwazr.binder.setter.FieldSetter;
import com.qwazr.database.TableServiceInterface;
import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.TableDefinition;
import com.qwazr.database.model.TableRequest;
import com.qwazr.database.model.TableRequestResult;
import com.qwazr.database.model.TableStatus;
import com.qwazr.database.store.KeyStore;
import com.qwazr.utils.AnnotationsUtils;
import com.qwazr.utils.StringUtils;
import org.apache.commons.lang3.SystemUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class AnnotatedTableService<T> extends FieldMapWrapper<T> {

    protected final TableServiceInterface tableService;

    protected final String tableName;

    private final Map<String, TableColumn> tableColumnMap;

    /**
     * Create a new index service. A class with Index and IndexField annotations.
     *
     * @param tableService         the IndexServiceInterface to use
     * @param tableDefinitionClass an annotated class
     * @param tableName            the name of table
     * @throws NoSuchMethodException if any reflection issue occurs
     */
    public AnnotatedTableService(final TableServiceInterface tableService, final Class<T> tableDefinitionClass,
                                 final String tableName) throws NoSuchMethodException {
        super(new LinkedHashMap<>(), tableDefinitionClass);
        Objects.requireNonNull(tableService, "The tableService parameter is null");
        Objects.requireNonNull(tableDefinitionClass, "The tableDefinitionClass parameter is null");
        this.tableService = tableService;
        final Table table = tableDefinitionClass.getAnnotation(Table.class);
        Objects.requireNonNull(table, "This class does not declare any Table annotation: " + tableDefinitionClass);

        this.tableName = tableName != null ? tableName : table.value();

        tableColumnMap = new LinkedHashMap<>();
        AnnotationsUtils.browseFieldsRecursive(tableDefinitionClass, field -> {
            if (!field.isAnnotationPresent(TableColumn.class))
                return;
            field.setAccessible(true);
            final TableColumn tableColumn = field.getDeclaredAnnotation(TableColumn.class);
            final String columnName = StringUtils.isEmpty(tableColumn.name()) ? field.getName() : tableColumn.name();
            tableColumnMap.put(columnName, tableColumn);
            fieldMap.put(columnName, FieldSetter.of(field));
        });
    }

    public AnnotatedTableService(final TableServiceInterface tableService, final Class<T> tableDefinitionClass)
            throws NoSuchMethodException {
        this(tableService, tableDefinitionClass, null);
    }

    public String getTableName() {
        return tableName;
    }

    private void checkParameters() {
        if (StringUtils.isEmpty(tableName))
            throw new RuntimeException("The table name is empty");
    }

    public void createUpdateTable(KeyStore.Impl keyStore) {
        checkParameters();
        if (tableService.list().contains(tableName))
            return;
        tableService.createTable(tableName, keyStore);
    }

    public void createUpdateTable() {
        createUpdateTable(SystemUtils.IS_OS_WINDOWS ? KeyStore.Impl.lmdb : KeyStore.Impl.leveldb);
    }

    public TableStatus getTableStatus() {
        checkParameters();
        return tableService.getTableStatus(tableName);
    }

    public void deleteTable() {
        checkParameters();
        tableService.deleteTable(tableName);
    }

    private LinkedHashMap<String, ColumnDefinition> getAnnotatedFields() {
        final LinkedHashMap<String, ColumnDefinition> columnFields = new LinkedHashMap<>();
        if (tableColumnMap != null)
            tableColumnMap.forEach(
                    (name, propertyField) -> columnFields.put(name, new ColumnDefinition(propertyField)));
        return columnFields;
    }

    /**
     * Set a collection of fields by reading the annotated fields.
     */
    public void createUpdateFields() {
        checkParameters();
        getColumnChanges().forEach((columnName, fieldStatus) -> {
            switch (fieldStatus) {
                case NOT_IDENTICAL:
                    tableService.removeColumn(tableName, columnName);
                    tableService.setColumn(tableName, columnName, new ColumnDefinition(tableColumnMap.get(columnName)));
                    break;
                case EXISTS_ONLY_IN_ANNOTATION:
                    tableService.setColumn(tableName, columnName, new ColumnDefinition(tableColumnMap.get(columnName)));
                    break;
                case EXISTS_ONLY_IN_TABLE:
                    tableService.removeColumn(tableName, columnName);
                    break;
                default:
                    break;
            }
        });
    }

    /**
     * Check if the there is differences between the annotated fields and the fields already declared
     *
     * @return a collection of changed fields
     */
    public Map<String, FieldStatus> getColumnChanges() {
        checkParameters();
        final LinkedHashMap<String, ColumnDefinition> annotatedFields = getAnnotatedFields();
        final Map<String, ColumnDefinition> tableColumns = tableService.getColumns(tableName);
        final HashMap<String, FieldStatus> fieldChanges = new HashMap<>();
        if (tableColumnMap != null) {
            tableColumnMap.forEach((name, propertyField) -> {
                final ColumnDefinition annotatedField = annotatedFields.get(name);
                final ColumnDefinition tableColumn = tableColumns == null ? null : tableColumns.get(name);
                if (name.equals(TableDefinition.ID_COLUMN_NAME)) // Ignore primary key
                    return;
                if (tableColumn == null) {
                    fieldChanges.put(name, FieldStatus.EXISTS_ONLY_IN_ANNOTATION);
                } else if (!Objects.equals(tableColumn.mode, annotatedField.mode) ||
                        !Objects.equals(tableColumn.type, annotatedField.type))
                    fieldChanges.put(name, FieldStatus.NOT_IDENTICAL);
            });
        }
        if (tableColumns != null) {
            tableColumns.forEach((name, tableColumn) -> {
                if (!annotatedFields.containsKey(name))
                    fieldChanges.put(name, FieldStatus.EXISTS_ONLY_IN_TABLE);
            });
        }
        return fieldChanges;
    }

    public ColumnDefinition getColumn(final String columnName) {
        checkParameters();
        return tableService.getColumn(tableName, columnName);
    }

    public Map<String, ColumnDefinition> getColumns() {
        checkParameters();
        return tableService.getColumns(tableName);
    }

    public List<Object> getColumnTerms(final String columnName, final Integer start, final Integer rows) {
        checkParameters();
        return tableService.getColumnTerms(tableName, columnName, start, rows);
    }

    public List<String> getColumnTermKeys(final String columnName, final String term, final Integer start,
                                          final Integer rows) {
        checkParameters();
        return tableService.getColumnTermKeys(tableName, columnName, term, start, rows);
    }

    public List<String> getRows(final Integer start, final Integer rows) {
        checkParameters();
        return tableService.getRows(tableName, start, rows);
    }

    public List<T> getRows(final Set<String> columns, final Set<String> rowIds)
            throws IOException, ReflectiveOperationException {
        checkParameters();
        return toRecords(tableService.getRows(tableName, columns, rowIds));
    }

    public Long upsertRows(final List<T> rows) {
        checkParameters();
        return tableService.upsertRows(tableName, newMapCollection(rows));
    }

    public void upsertRow(final String rowId, final T row) {
        checkParameters();
        tableService.upsertRow(tableName, rowId, newMap(row));
    }

    public T getRow(final String rowId, final Set<String> columns) throws ReflectiveOperationException, IOException {
        checkParameters();
        return toRecord(tableService.getRow(tableName, rowId, columns));
    }

    public Boolean deleteRow(final String rowId) {
        checkParameters();
        return tableService.deleteRow(tableName, rowId);
    }

    public TableRequestResultRecords<T> queryRows(final TableRequest tableRequest)
            throws IOException, ReflectiveOperationException {
        checkParameters();
        final TableRequestResult result = tableService.queryRows(tableName, tableRequest);
        return result == null ? null : new TableRequestResultRecords<>(result, toRecords(result.rows));
    }

}
