/*
 * Copyright 2015-2018 Emmanuel Keller / QWAZR
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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.qwazr.utils.CollectionsUtils;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.PUBLIC_ONLY,
		isGetterVisibility = JsonAutoDetect.Visibility.NONE,
		getterVisibility = JsonAutoDetect.Visibility.NONE,
		setterVisibility = JsonAutoDetect.Visibility.NONE,
		creatorVisibility = JsonAutoDetect.Visibility.NONE)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@type")
@JsonSubTypes({ @JsonSubTypes.Type(value = TableQuery.StringTerm.class, name = "string"),
		@JsonSubTypes.Type(value = TableQuery.DoubleTerm.class, name = "double"),
		@JsonSubTypes.Type(value = TableQuery.FloatTerm.class, name = "float"),
		@JsonSubTypes.Type(value = TableQuery.LongTerm.class, name = "long"),
		@JsonSubTypes.Type(value = TableQuery.IntegerTerm.class, name = "integer"),
		@JsonSubTypes.Type(value = TableQuery.Or.class, name = "or"),
		@JsonSubTypes.Type(value = TableQuery.And.class, name = "and") })
public class TableQuery {

	public static abstract class Term<T> extends TableQuery {

		public final String column;
		public final T value;

		@JsonCreator
		private Term(final String column, final T value) {
			this.column = column;
			this.value = value;
		}

		@Override
		final public boolean equals(final Object o) {
			if (o == null || !(o instanceof Term))
				return false;
			if (o == this)
				return true;
			final Term t = (Term) o;
			return Objects.equals(column, t.column) && Objects.equals(value, t.value);
		}

		public String getColumn() {
			return column;
		}

		public T getValue() {
			return value;
		}
	}

	final static public class StringTerm extends Term<String> {

		@JsonCreator
		public StringTerm(@JsonProperty("column") final String column, @JsonProperty("value") final String value) {
			super(column, value);
		}
	}

	final static public class DoubleTerm extends Term<Double> {

		@JsonCreator
		public DoubleTerm(@JsonProperty("column") final String column, @JsonProperty("value") final Double value) {
			super(column, value);
		}
	}

	final static public class FloatTerm extends Term<Float> {

		@JsonCreator
		public FloatTerm(@JsonProperty("column") final String column, @JsonProperty("value") final Float value) {
			super(column, value);
		}
	}

	final static public class LongTerm extends Term<Long> {

		@JsonCreator
		public LongTerm(@JsonProperty("column") final String column, @JsonProperty("value") final Long value) {
			super(column, value);
		}
	}

	final static public class IntegerTerm extends Term<Integer> {

		@JsonCreator
		public IntegerTerm(@JsonProperty("column") final String column, @JsonProperty("value") final Integer value) {
			super(column, value);
		}
	}

	static public class Group extends TableQuery {

		public final LinkedHashSet<TableQuery> queries;

		private Group(final LinkedHashSet<TableQuery> queries) {
			this.queries = queries;
		}

		final public Group add(final String column, final String term) {
			queries.add(new StringTerm(column, term));
			return this;
		}

		final public Group add(final String column, final Long term) {
			queries.add(new LongTerm(column, term));
			return this;
		}

		final public Group add(final String column, final Integer term) {
			queries.add(new IntegerTerm(column, term));
			return this;
		}

		final public Group add(final String column, final Double term) {
			queries.add(new DoubleTerm(column, term));
			return this;
		}

		final public Group add(final String column, final Float term) {
			queries.add(new FloatTerm(column, term));
			return this;
		}

		final public Group add(final Group group) {
			queries.add(group);
			return this;
		}

		@Override
		final public boolean equals(final Object o) {
			if (o == null || !(o instanceof Group))
				return false;
			if (o == this)
				return true;
			final Group g = (Group) o;
			return CollectionsUtils.equals(queries, g.queries);
		}

		public Set<TableQuery> getQueries() {
			return queries;
		}
	}

	public final static class Or extends Group {

		@JsonCreator
		private Or(@JsonProperty("queries") LinkedHashSet<TableQuery> queries) {
			super(queries);
		}

		public Or() {
			this(new LinkedHashSet<>());
		}
	}

	public final static class And extends Group {

		@JsonCreator
		private And(@JsonProperty("queries") LinkedHashSet<TableQuery> queries) {
			super(queries);
		}

		public And() {
			this(new LinkedHashSet<>());
		}
	}

}
