/*
 * Copyright 2016-2017 Emmanuel Keller / QWAZR
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
package com.qwazr.database;

import com.qwazr.database.annotations.Table;
import com.qwazr.database.annotations.TableColumn;
import com.qwazr.database.model.ColumnDefinition;
import com.qwazr.database.model.TableDefinition;
import com.qwazr.utils.RandomUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

@Table("JavaTest")
public class JavaRecord {

	@TableColumn(name = TableDefinition.ID_COLUMN_NAME,
			mode = ColumnDefinition.Mode.INDEXED,
			type = ColumnDefinition.Type.STRING)
	public final String id;

	public final static String COL_DPT = "dpt";
	@TableColumn(name = COL_DPT, mode = ColumnDefinition.Mode.INDEXED, type = ColumnDefinition.Type.INTEGER)
	public final ArrayList<Integer> dpt;

	public final static String COL_LABEL = "label";
	@TableColumn(name = COL_LABEL, mode = ColumnDefinition.Mode.STORED, type = ColumnDefinition.Type.STRING)
	public final String label;

	public final static String COL_LAST_UPDATE_DATE = "lastUpdateDate";
	@TableColumn(name = COL_LAST_UPDATE_DATE, mode = ColumnDefinition.Mode.INDEXED, type = ColumnDefinition.Type.LONG)
	public final Long lastUpdateDate;

	public final static String COL_CREATION_DATE = "creationDate";
	@TableColumn(name = COL_CREATION_DATE, mode = ColumnDefinition.Mode.STORED, type = ColumnDefinition.Type.LONG)
	public final Long creationDate;

	public JavaRecord(final String id, final int dptStart) {
		this.id = id;
		dpt = new ArrayList<>(
				Arrays.asList(dptStart + RandomUtils.nextInt(0, 9), dptStart + RandomUtils.nextInt(10, 19),
						dptStart + RandomUtils.nextInt(20, 29)));
		label = RandomUtils.alphanumeric(16);
		lastUpdateDate = System.currentTimeMillis() + RandomUtils.nextInt(0, 86400000);
		creationDate = System.currentTimeMillis() + RandomUtils.nextInt(0, 86400000);
	}

	public JavaRecord() {
		id = null;
		dpt = null;
		label = null;
		lastUpdateDate = null;
		creationDate = null;
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || !(o instanceof JavaRecord))
			return false;
		final JavaRecord r = (JavaRecord) o;
		if (!Objects.deepEquals(dpt, r.dpt))
			return false;
		if (!Objects.equals(label, r.label))
			return false;
		if (!Objects.equals(lastUpdateDate, r.lastUpdateDate))
			return false;
		if (!Objects.equals(creationDate, r.creationDate))
			return false;
		return true;
	}

}
