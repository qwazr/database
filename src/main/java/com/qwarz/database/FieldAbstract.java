/**
 * Copyright 2015 Emmanuel Keller / QWAZR
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qwarz.database;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class FieldAbstract<T> implements FieldInterface<T> {

	protected static final Logger logger = LoggerFactory
			.getLogger(FieldAbstract.class);

	protected final String name;
	protected final long fieldId;

	FieldAbstract(String name, long fieldId) {
		this.name = name;
		this.fieldId = fieldId;
		logger.info("Create field (" + fieldId + "): " + name + " "
				+ this.getClass().getName());
	}

	@Override
	public void commit() throws IOException {
	}

	@Override
	public void delete() throws IOException {
	}

}