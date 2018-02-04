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
package com.qwazr.database.test;

import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.TableDefinition;
import com.qwazr.database.model.TableStatus;
import org.junit.Assert;

public interface TableTestHelper {

	default TableStatus checkStatus(final TableStatus status, final Integer numRows,
			final TableDefinition tableDefinition) {
		Assert.assertNotNull(status);
		Assert.assertEquals(numRows, status.numRows);
		Assert.assertEquals(numRows, status.getNumRows());
		Assert.assertEquals(tableDefinition, status.definition);
		Assert.assertEquals(tableDefinition, status.getDefinition());
		Assert.assertEquals(tableDefinition.implementation, status.getDefinition().getImplementation());
		Assert.assertEquals(tableDefinition.columns, status.getDefinition().getColumns());
		Assert.assertEquals(status, new TableStatus(tableDefinition, numRows));
		Assert.assertNotEquals(status, null);
		Assert.assertNotEquals(status.definition, null);
		return status;
	}

	default ColumnDefinition checkColumn(final ColumnDefinition column, final ColumnDefinition.Type expectedType,
			final ColumnDefinition.Mode expectedMode) {
		Assert.assertNotNull(column);
		Assert.assertEquals(expectedMode, column.mode);
		Assert.assertEquals(expectedMode, column.getMode());
		Assert.assertEquals(expectedType, column.type);
		Assert.assertEquals(expectedType, column.getType());
		Assert.assertEquals(column, new ColumnDefinition(expectedType, expectedMode));
		Assert.assertNotEquals(column, null);
		return column;
	}
}
