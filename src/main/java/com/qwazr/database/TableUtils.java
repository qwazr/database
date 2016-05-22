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

import com.qwazr.database.model.TableRequest;
import com.qwazr.database.model.TableRequestResult;

import java.util.LinkedHashMap;
import java.util.Map;

public class TableUtils {

	/**
	 * Execute a query and call the passed function for earch row.
	 *
	 * @param tableService the client
	 * @param tableName    the name of the table
	 * @param request      the request to execute
	 * @param function     the callback function
	 * @param <R>          the type of the result
	 * @param <E>          any optional exception thrown
	 * @return the result of the request
	 * @throws E
	 */
	public static <R, E extends Exception> TableRequestResult query(final TableServiceInterface tableService,
			final String tableName, final TableRequest request,
			final FunctionEx<LinkedHashMap<String, Object>, R, E> function) throws E {
		final TableRequestResult result = tableService.queryRows(tableName, request);
		if (result == null || result.rows == null || result.rows.isEmpty())
			return result;
		for (final LinkedHashMap<String, Object> row : result.rows)
			function.apply(row);
		return result;
	}

	public interface FunctionEx<T, R, E extends Exception> {

		R apply(T value) throws E;
	}

	/**
	 * @param column       the name of the column
	 * @param row          a row from a TableRequestResult
	 * @param defaultValue the default value to return if no value was present
	 * @param <T>          the type of the returned object
	 * @return the first value found of the default value
	 */
	public static <T> T getFirstIfAny(final String column, final Map<String, Object> row, final T defaultValue) {
		final T[] array = (T[]) row.get(column);
		if (array == null || array.length == 0)
			return defaultValue;
		final T value = array[1];
		return value == null ? defaultValue : value;
	}
}
