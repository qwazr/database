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
import com.qwazr.database.model.TableDefinition;
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

	private final Map<String, TableColumn> tableColumnMap;

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

		tableColumnMap = new LinkedHashMap<>();
		AnnotationsUtils.browseFieldsRecursive(tableDefinitionClass, field -> {
			if (!field.isAnnotationPresent(TableColumn.class))
				return;
			field.setAccessible(true);
			final TableColumn tableColumn = field.getDeclaredAnnotation(TableColumn.class);
			final String columnName = StringUtils.isEmpty(tableColumn.name()) ? field.getName() : tableColumn.name();
			tableColumnMap.put(columnName, tableColumn);
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
		if (tableService.list().contains(tableName))
			return;
		tableService.createTable(tableName, KeyStore.Impl.leveldb);
	}

	public TableDefinition getTable() {
		checkParameters();
		return tableService.getTable(tableName);
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
	 *
	 * @return the field map
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

	public enum FieldStatus {
		NOT_IDENTICAL, EXISTS_ONLY_IN_TABLE, EXISTS_ONLY_IN_ANNOTATION
	}

	/**
	 * Check if the there is differences between the annotated fields and the fields already declared
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
				if (tableColumn == null) {
					if (!name.equals(TableDefinition.ID_COLUMN_NAME))
						fieldChanges.put(name, FieldStatus.EXISTS_ONLY_IN_ANNOTATION);
				} else if (!Objects.equals(tableColumn.mode, annotatedField.mode) || !Objects.equals(tableColumn.type,
						annotatedField.type))
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

	public List<Map<String, Object>> getRows(final Set<String> columns, final Set<String> rowIds) {
		checkParameters();
		return tableService.getRows(tableName, columns, rowIds);
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

	public TableRequestResultRecords<T> queryRows(final TableRequest tableRequest) {
		checkParameters();
		final TableRequestResult result = tableService.queryRows(tableName, tableRequest);
		return result == null ? null : new TableRequestResultRecords(result, toRecords(result.rows));
	}

	public static class TableRequestResultRecords<T> extends TableRequestResult {

		final public List<T> records;

		private TableRequestResultRecords(final TableRequestResult result, final List<T> records) {
			super(result);
			this.records = records;
		}

	}
}
