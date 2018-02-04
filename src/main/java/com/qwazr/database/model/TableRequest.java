/*
 * Copyright 2015-2017 Emmanuel Keller / QWAZR
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
import com.qwazr.utils.CollectionsUtils;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

@JsonInclude(Include.NON_EMPTY)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY,
		isGetterVisibility = JsonAutoDetect.Visibility.NONE,
		getterVisibility = JsonAutoDetect.Visibility.NONE,
		setterVisibility = JsonAutoDetect.Visibility.NONE,
		creatorVisibility = JsonAutoDetect.Visibility.NONE)
public class TableRequest {

	public final Integer start;
	public final Integer rows;

	public final Set<String> columns;
	public final Set<String> counters;

	public final TableQuery query;

	@JsonCreator
	private TableRequest(@JsonProperty("start") Integer start, @JsonProperty("rows") Integer rows,
			@JsonProperty("columns") Set<String> columns, @JsonProperty("counters") Set<String> counters,
			@JsonProperty("query") TableQuery query) {
		this.start = start;
		this.rows = rows;
		this.columns = columns;
		this.counters = counters;
		this.query = query;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || !(o instanceof TableRequest))
			return false;
		if (o == this)
			return true;
		final TableRequest r = (TableRequest) o;
		return Objects.equals(start, r.start) && Objects.equals(rows, r.rows) &&
				CollectionsUtils.equals(columns, r.columns) && CollectionsUtils.equals(counters, r.counters) &&
				Objects.equals(query, r.query);
	}

	public Integer getStart() {
		return start;
	}

	public Integer getRows() {
		return rows;
	}

	public Set<String> getColumns() {
		return columns;
	}

	public Set<String> getCounters() {
		return counters;
	}

	public TableQuery getQuery() {
		return query;
	}

	public static Builder from(Integer start, Integer rows) {
		return new Builder().start(start).rows(rows);
	}

	public static class Builder {

		private Integer start;
		private Integer rows;

		private Set<String> columns;
		private Set<String> counters;

		private TableQuery query;

		public Builder start(Integer start) {
			this.start = start;
			return this;
		}

		public Builder rows(Integer rows) {
			this.rows = rows;
			return this;
		}

		public Builder column(String... columns) {
			if (columns != null && columns.length > 0) {
				if (this.columns == null)
					this.columns = new LinkedHashSet<>();
				Collections.addAll(this.columns, columns);
			}
			return this;
		}

		public Builder counter(String... counters) {
			if (counters != null && counters.length > 0) {
				if (this.counters == null)
					this.counters = new LinkedHashSet<>();
				Collections.addAll(this.counters, counters);
			}
			return this;
		}

		public Builder query(TableQuery query) {
			this.query = query;
			return this;
		}

		public TableRequest build() {
			return new TableRequest(start, rows, columns, counters, query);
		}
	}

}