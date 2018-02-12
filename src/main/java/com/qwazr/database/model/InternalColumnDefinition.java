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

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NON_PRIVATE,
		getterVisibility = JsonAutoDetect.Visibility.NONE,
		setterVisibility = JsonAutoDetect.Visibility.NONE,
		isGetterVisibility = JsonAutoDetect.Visibility.NONE,
		creatorVisibility = JsonAutoDetect.Visibility.NONE)
public class InternalColumnDefinition extends ColumnDefinition {

	@JsonProperty("column_id")
	public final int columnId;

	@JsonCreator
	public InternalColumnDefinition(@JsonProperty("type") final Type type, @JsonProperty("mode") final Mode mode,
			@JsonProperty("column_id") int columnId) {
		super(type, mode);
		this.columnId = columnId;
	}

	public InternalColumnDefinition(ColumnDefinition columnDefinition, int columnId) {
		super(columnDefinition);
		this.columnId = columnId;
	}

	public final static InternalColumnDefinition PRIMARY_KEY =
			new InternalColumnDefinition(ColumnDefinition.ID_COLUMN_DEF, 0);

}
