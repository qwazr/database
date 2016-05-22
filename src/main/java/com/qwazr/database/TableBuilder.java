/**
 * Copyright 2015-2016 Emmanuel Keller / QWAZR
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

import com.qwazr.database.model.ColumnDefinition;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class TableBuilder {

	public final String tableName;

	public final Map<String, ColumnDefinition> columns;

	public TableBuilder(final String tableName) {
		this.tableName = tableName;
		columns = new LinkedHashMap<>();
	}

	public TableBuilder addColumn(final String name, final ColumnDefinition.Type type,
			final ColumnDefinition.Mode mode) {
		columns.put(name, new ColumnDefinition(type, mode));
		return this;
	}

	/**
	 * The build do the following tasks:
	 * <ul>
	 * <li>Create the table if it does not exist.</li>
	 * <li>Create the columns if they does not exist.</li>
	 * <li>Remove existing columns if they are not defined.</li>
	 * </ul>
	 *
	 * @param tableService
	 */
	public void build(final TableServiceInterface tableService) {
		final Set<String> tables = tableService.list(null, null);
		if (!tables.contains(tableName))
			tableService.createTable(tableName);
		final Map<String, ColumnDefinition> existingColumns = tableService.getColumns(tableName);
		columns.forEach((columnName, columnDefinition) -> {
			if (!existingColumns.containsKey(columnName))
				tableService.addColumn(tableName, columnName, columnDefinition);
		});
		existingColumns.keySet().forEach(columnName -> {
			if (!columns.containsKey(columnName))
				tableService.removeColumn(tableName, columnName);
		});
	}

}
