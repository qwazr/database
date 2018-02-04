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
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NON_PRIVATE,
		getterVisibility = JsonAutoDetect.Visibility.NONE,
		setterVisibility = JsonAutoDetect.Visibility.NONE,
		isGetterVisibility = JsonAutoDetect.Visibility.NONE,
		creatorVisibility = JsonAutoDetect.Visibility.NONE)
public class TableStatus {

	public final TableDefinition definition;

	@JsonProperty("num_rows")
	public final Integer numRows;

	@JsonCreator
	public TableStatus(final @JsonProperty("definition") TableDefinition definition,
			final @JsonProperty("num_rows") Integer numRows) {
		this.definition = definition;
		this.numRows = numRows;
	}

	public TableDefinition getDefinition() {
		return definition;
	}

	public Integer getNumRows() {
		return numRows;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || !(o instanceof TableStatus))
			return false;
		if (o == this)
			return true;
		final TableStatus s = (TableStatus) o;
		return Objects.equals(definition, s.definition) && Objects.equals(numRows, s.numRows);
	}
}
