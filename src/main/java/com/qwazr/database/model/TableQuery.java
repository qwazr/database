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

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.qwazr.utils.ObjectMappers;

import java.util.LinkedHashSet;
import java.util.Set;

public abstract class TableQuery {

	protected abstract ObjectNode build();

	private static abstract class Term<T> extends TableQuery {

		protected final String column;
		protected final T value;

		private Term(final String column, final T value) {
			this.column = column;
			this.value = value;
		}
	}

	private static class StringTerm extends Term<String> {

		private StringTerm(final String column, final String value) {
			super(column, value);
		}

		@Override
		final protected ObjectNode build() {
			return ObjectMappers.JSON.createObjectNode().put(column, value);
		}
	}

	private static class DoubleTerm extends Term<Double> {

		private DoubleTerm(final String column, final Double value) {
			super(column, value);
		}

		@Override
		final protected ObjectNode build() {
			return ObjectMappers.JSON.createObjectNode().put(column, value);
		}
	}

	private static class FloatTerm extends Term<Float> {

		private FloatTerm(final String column, final Float value) {
			super(column, value);
		}

		@Override
		final protected ObjectNode build() {
			return ObjectMappers.JSON.createObjectNode().put(column, value);
		}
	}

	private static class LongTerm extends Term<Long> {

		private LongTerm(final String column, final Long value) {
			super(column, value);
		}

		@Override
		final protected ObjectNode build() {
			return ObjectMappers.JSON.createObjectNode().put(column, value);
		}
	}

	private static class IntegerTerm extends Term<Integer> {

		private IntegerTerm(final String column, final Integer value) {
			super(column, value);
		}

		@Override
		final protected ObjectNode build() {
			return ObjectMappers.JSON.createObjectNode().put(column, value);
		}
	}

	public abstract static class Group extends TableQuery {

		private final String command;
		private final Set<TableQuery> queries;

		protected Group(final String command) {
			this.command = command;
			queries = new LinkedHashSet<>();
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
		final public ObjectNode build() {
			final ArrayNode array = ObjectMappers.JSON.createArrayNode();
			for (TableQuery q : queries)
				array.add(q.build());
			final ObjectNode object = ObjectMappers.JSON.createObjectNode();
			object.set(command, array);
			return object;
		}
	}

	public static class Or extends Group {

		public Or() {
			super("$OR");
		}
	}

	public static class And extends Group {

		public And() {
			super("$AND");
		}
	}

}
