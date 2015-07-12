/**
 * Copyright 2015 Emmanuel Keller / QWAZR
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwazr.database;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

abstract class ColumnAbstract<T> implements ColumnInterface<T> {

	protected static final Logger logger = LoggerFactory
			.getLogger(ColumnAbstract.class);

	protected final String name;
	protected final long columnId;

	ColumnAbstract(String name, long columnId) {
		this.name = name;
		this.columnId = columnId;
		logger.info("Load column (" + columnId + "): " + name + " "
				+ this.getClass().getName());
	}

	@Override
	public void commit() throws IOException {
	}

	@Override
	public void delete() throws IOException {
	}

}