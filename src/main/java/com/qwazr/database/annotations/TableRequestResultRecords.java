/*
 * Copyright 2016-2018 Emmanuel Keller / QWAZR
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

import com.qwazr.database.model.TableRequestResult;

import java.util.Collections;
import java.util.List;

public class TableRequestResultRecords<T> extends TableRequestResult {

	final public List<T> records;

	TableRequestResultRecords(final TableRequestResult result, final List<T> records) {
		super(result.count, result.rows, result.counters);
		this.records = records == null ? null : Collections.unmodifiableList(records);
	}

	public List<T> getRecords() {
		return records;
	}

}