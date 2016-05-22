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

import javax.ws.rs.WebApplicationException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

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
			final String tableName, final TableRequest request, final FunctionEx<Map<String, Object>, R, E> function)
			throws E {
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
	 * @return the first value found or the default value
	 */
	public static String getFirstStringIfAny(final String column, final Map<String, Object> row,
			final String defaultValue) {
		final String[] array = (String[]) row.get(column);
		if (array != null && array.length > 0)
			return array[0];
		return defaultValue;
	}

	/**
	 * @param column       the name of the column
	 * @param row          a row from a TableRequestResult
	 * @param defaultValue the default value to return if no value was present
	 * @return the first value found or the default value
	 */
	public static Integer getFirstIntegerIfAny(final String column, final Map<String, Object> row,
			final Integer defaultValue) {
		final int[] array = (int[]) row.get(column);
		if (array != null && array.length > 0)
			return array[0];
		return defaultValue;
	}

	/**
	 * @param column       the name of the column
	 * @param row          a row from a TableRequestResult
	 * @param defaultValue the default value to return if no value was present
	 * @return the first value found or the default value
	 */
	public static Long getFirstLongIfAny(final String column, final Map<String, Object> row, final Long defaultValue) {
		final long[] array = (long[]) row.get(column);
		if (array != null && array.length > 0)
			return array[0];
		return defaultValue;
	}

	/**
	 * @param column       the name of the column
	 * @param row          a row from a TableRequestResult
	 * @param defaultValue the default value to return if no value was present
	 * @return the first value found or the default value
	 */
	public static Double getFirstDoubleIfAny(final String column, final Map<String, Object> row,
			final Double defaultValue) {
		final double[] array = (double[]) row.get(column);
		if (array != null && array.length > 0)
			return array[0];
		return defaultValue;
	}

	/**
	 * Return the row for the given id
	 *
	 * @param tableService the client
	 * @param tableName    the name of the table
	 * @param id           the id of the requested row
	 * @param columns      the columns to return
	 * @param function     the callback function
	 * @param <R>          the type of the result
	 * @param <E>          any optional exception thrown
	 * @return the row for the given id, or null if no row exists
	 * @throws E
	 */
	public static <R, E extends Exception> R getRow(final TableServiceInterface tableService, final String tableName,
			final String id, final Set<String> columns, final FunctionEx<Map<String, Object>, R, E> function) throws E {
		final Map<String, Object> row;
		try {
			row = tableService.getRow(tableName, id, columns);
		} catch (WebApplicationException e) {
			if (e.getResponse().getStatus() == 404)
				return null;
			throw e;
		}
		if (row == null || row.isEmpty())
			return null;
		return function.apply(row);
	}
}
