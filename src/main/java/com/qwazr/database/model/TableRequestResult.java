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
package com.qwazr.database.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

@JsonInclude(Include.NON_EMPTY)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY,
		isGetterVisibility = JsonAutoDetect.Visibility.NONE,
		getterVisibility = JsonAutoDetect.Visibility.NONE,
		setterVisibility = JsonAutoDetect.Visibility.NONE,
		creatorVisibility = JsonAutoDetect.Visibility.NONE)
public class TableRequestResult {

	final public Long count;

	final public List<Map<String, Object>> rows;
	final public Map<String, Map<String, Long>> counters;

	@JsonCreator
	public TableRequestResult(@JsonProperty("count") Long count, @JsonProperty("rows") List<Map<String, Object>> rows,
			@JsonProperty("counters") Map<String, Map<String, Long>> counters) {
		this.count = count;
		this.rows = rows;
		this.counters = counters;
	}

	public Long getCount() {
		return count;
	}

	public List<Map<String, Object>> getRows() {
		return rows;
	}

	public Map<String, Map<String, Long>> getCounters() {
		return counters;
	}

	public static class Builder {

		private final Long count;
		private List<Map<String, Object>> rows;
		private Map<String, Map<String, Long>> counters;

		public Builder(final Long count) {
			this.count = count;
		}

		public Builder rows(final List<Map<String, Object>> rows) {
			this.rows = rows;
			return this;
		}

		public Builder counters(final Map<String, Map<String, Long>> counters) {
			this.counters = counters;
			return this;
		}

		public TableRequestResult build() {
			return new TableRequestResult(count, rows, counters);
		}

	}
}