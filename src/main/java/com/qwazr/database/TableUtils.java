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
import java.util.function.Function;

public class TableUtils {

	public static <T> TableRequestResult query(final TableServiceInterface tableService, final String tableName,
			final TableRequest request, final Function<LinkedHashMap<String, Object>, T> function) {
		final TableRequestResult result = tableService.queryRows(tableName, request);
		if (result == null || result.rows == null || result.rows.isEmpty())
			return result;
		result.rows.forEach(row -> function.apply(row));
		return result;
	}
}
