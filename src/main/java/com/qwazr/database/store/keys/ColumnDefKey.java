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
 **/
package com.qwazr.database.store.keys;

import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.store.ByteConverter;
import com.qwazr.utils.CharsetUtils;

import java.io.DataOutputStream;
import java.io.IOException;

final public class ColumnDefKey extends KeyAbstract<ColumnDefinition.Internal> {

	private final byte[] fieldBytes;

	static final ByteConverter.JsonByteConverter<ColumnDefinition.Internal> columnInternalDefinitionByteConverter = new ByteConverter.JsonByteConverter<>(
			ColumnDefinition.Internal.class);

	public ColumnDefKey(String fieldName) {
		super(KeyEnum.COLUMN_DEF, columnInternalDefinitionByteConverter);
		this.fieldBytes = CharsetUtils.encodeUtf8(fieldName);
	}

	@Override
	final public void buildKey(final DataOutputStream output) throws IOException {
		super.buildKey(output);
		output.write(fieldBytes);
	}

}
