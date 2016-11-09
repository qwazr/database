/**
 * Copyright 2016 Emmanuel Keller / QWAZR
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

import com.qwazr.database.TableServiceInterface;
import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.TableRequest;
import com.qwazr.database.model.TableRequestResult;
import com.qwazr.database.store.KeyStore;
import com.qwazr.utils.AnnotationsUtils;
import com.qwazr.utils.FieldMapWrapper;
import com.qwazr.utils.StringUtils;

import java.net.URISyntaxException;
import java.util.*;

public class AnnotatedTableService<T> extends FieldMapWrapper<T> {

  protected final TableServiceInterface tableService;

  private final Class<T> tableDefinitionClass;

  protected final String tableName;

  private final Map<String, TableColumn> columnMap;

  /**
   * Create a new index service. A class with Index and IndexField annotations.
   *
   * @param tableService         the IndexServiceInterface to use
   * @param tableDefinitionClass an annotated class
   * @param tableName            the name of table
   */
  public AnnotatedTableService(final TableServiceInterface tableService, final Class<T> tableDefinitionClass,
          final String tableName) throws URISyntaxException {
    super(new LinkedHashMap<>(), tableDefinitionClass);
    Objects.requireNonNull(tableService, "The tableService parameter is null");
    Objects.requireNonNull(tableDefinitionClass, "The tableDefinitionClass parameter is null");
    this.tableService = tableService;
    this.tableDefinitionClass = tableDefinitionClass;
    final Table table = tableDefinitionClass.getAnnotation(Table.class);
    Objects.requireNonNull(table, "This class does not declare any Table annotation: " + tableDefinitionClass);

    this.tableName = tableName != null ? tableName : table.value();

    columnMap = new LinkedHashMap<>();
    AnnotationsUtils.browseFieldsRecursive(tableDefinitionClass, field -> {
      if (!field.isAnnotationPresent(TableColumn.class))
        return;
      field.setAccessible(true);
      final TableColumn tableColumn = field.getDeclaredAnnotation(TableColumn.class);
      final String columnName = StringUtils.isEmpty(tableColumn.name()) ? field.getName() : tableColumn.name();
      columnMap.put(columnName, tableColumn);
      fieldMap.put(columnName, field);
    });
  }

  public AnnotatedTableService(final TableServiceInterface tableService, final Class<T> tableDefinitionClass)
          throws URISyntaxException {
    this(tableService, tableDefinitionClass, null);
  }

  public String getTableName() {
    return tableName;
  }

  final private void checkParameters() {
    if (StringUtils.isEmpty(tableName))
      throw new RuntimeException("The table name is empty");
  }

  public void createUpdateTable() {
    checkParameters();
    tableService.createTable(tableName, KeyStore.Impl.leveldb);
  }

  public void deleteTable() {
    checkParameters();
    tableService.deleteTable(tableName);
  }

  private LinkedHashMap<String, ColumnDefinition> getAnnotatedFields() {
    final LinkedHashMap<String, ColumnDefinition> columnFields = new LinkedHashMap<>();
    if (columnMap != null)
      columnMap.forEach((name, propertyField) -> columnFields.put(name, new ColumnDefinition(propertyField)));
    return columnFields;
  }

  /**
   * Set a collection of fields by reading the annotated fields.
   *
   * @return the field map
   */
  public LinkedHashMap<String, ColumnDefinition> createUpdateFields() {
    checkParameters();
    //TODO
    return null;
  }

  public enum FieldStatus {
    NOT_IDENTICAL, EXISTS_ONLY_IN_TABLE, EXISTS_ONLY_IN_ANNOTATION
  }

  /**
   * Check if the there is differences between the annotated fields and the fields already declared
   */
  public Map<String, FieldStatus> getFieldChanges() {
    checkParameters();
    final LinkedHashMap<String, ColumnDefinition> annotatedFields = getAnnotatedFields();
    final Map<String, ColumnDefinition> columns = tableService.getColumns(tableName);
    final HashMap<String, FieldStatus> fieldChanges = new HashMap<>();
    if (columns != null) {
      columns.forEach((name, propertyField) -> {
        final ColumnDefinition annotatedField = annotatedFields.get(name);
        final TableColumn columnField = columnMap == null ? null : columnMap.get(name);
        if (columnField == null)
          fieldChanges.put(name, FieldStatus.EXISTS_ONLY_IN_ANNOTATION);
        else if (!columnField.equals(annotatedField))
          fieldChanges.put(name, FieldStatus.NOT_IDENTICAL);
      });
    }
    if (columnMap != null) {
      columnMap.forEach((name, indexField) -> {
        if (!annotatedFields.containsKey(name))
          fieldChanges.put(name, FieldStatus.EXISTS_ONLY_IN_TABLE);
      });
    }
    return fieldChanges;
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

  public Long upsertRows(final List<T> rows) {
    checkParameters();
    return tableService.upsertRows(tableName, newMapCollection(rows));
  }

  public void upsertRow(final String rowId, final T row) {
    checkParameters();
    tableService.upsertRow(tableName, rowId, newMap(row));
  }

  public T getRow(final String rowId, final Set<String> columns) throws ReflectiveOperationException {
    checkParameters();
    return toRecord(tableService.getRow(tableName, rowId, columns));
  }

  public Boolean deleteRow(final String rowId) {
    checkParameters();
    return tableService.deleteRow(tableName, rowId);
  }

  public TableRequestResult queryRows(final TableRequest tableRequest) {
    checkParameters();
    return tableService.queryRows(tableName, tableRequest);
  }

}
