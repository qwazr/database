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
package com.qwazr.database.model;

public class ColumnDefinition {


	public static enum Type {
		STRING, DOUBLE, LONG, INTEGER;
	}

	public static enum Mode {
		INDEXED, STORED;
	}

	public final Type type;
	public final Mode mode;

	public ColumnDefinition() {
		this(null, null);
	}

	public ColumnDefinition(Type type, Mode mode) {
		this.type = type;
		this.mode = mode;
	}

}
