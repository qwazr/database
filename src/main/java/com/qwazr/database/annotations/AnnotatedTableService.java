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
import com.qwazr.utils.AnnotationsUtils;
import com.qwazr.utils.StringUtils;

import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class AnnotatedTableService<T> {

  protected final TableServiceInterface tableService;

  private final Class<T> tableDefinitionClass;

  protected final String tableName;

  private final Map<String, TableColumn> columnMap;

  private final Map<String, Field> fieldMap;

  /**
   * Create a new index service. A class with Index and IndexField annotations.
   *
   * @param tableService         the IndexServiceInterface to use
   * @param tableDefinitionClass an annotated class
   * @param tableName            the name of table
   */
  public AnnotatedTableService(final TableServiceInterface tableService, final Class<T> tableDefinitionClass,
          final String tableName) throws URISyntaxException {
    Objects.requireNonNull(tableService, "The tableService parameter is null");
    Objects.requireNonNull(tableDefinitionClass, "The tableDefinitionClass parameter is null");
    this.tableService = tableService;
    this.tableDefinitionClass = tableDefinitionClass;
    final Table table = tableDefinitionClass.getAnnotation(Table.class);
    Objects.requireNonNull(table, "This class does not declare any Table annotation: " + tableDefinitionClass);

    this.tableName = tableName != null ? tableName : table.value();

    fieldMap = new LinkedHashMap<>();
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

}
