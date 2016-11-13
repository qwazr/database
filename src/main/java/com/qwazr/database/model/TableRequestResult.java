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
package com.qwazr.database.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(Include.NON_EMPTY)
public class TableRequestResult {

	final public Long count;

	final public List<Map<String, Object>> rows;
	final public Map<String, Map<String, Long>> counters;

	public TableRequestResult() {
		count = null;
		rows = null;
		counters = null;
	}

	public TableRequestResult(final Long count) {
		this.count = count;
		this.rows = new ArrayList<>();
		this.counters = new LinkedHashMap<>();
	}

	public TableRequestResult(final TableRequestResult result) {
		this.count = result.count;
		this.rows = result.rows;
		this.counters = result.counters;
	}
}