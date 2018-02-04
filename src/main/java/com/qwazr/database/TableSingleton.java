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
package com.qwazr.database;

import com.qwazr.database.store.Tables;
import com.qwazr.utils.concurrent.ExecutorSingleton;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

public class TableSingleton extends ExecutorSingleton implements Closeable {

	private final Path parentDirectory;

	private volatile Path tablesDirectory;
	private volatile TableManager tableManager;

	public TableSingleton(final Path parentDirectory, final ExecutorService executorService) {
		super(executorService);
		this.parentDirectory = parentDirectory;
	}

	public Path getTablesDirectory() throws IOException {
		if (tablesDirectory == null) {
			synchronized (TableManager.class) {
				if (tablesDirectory == null)
					tablesDirectory = TableManager.checkTablesDirectory(parentDirectory);
			}
		}
		return tablesDirectory;
	}

	public TableManager getTableManager() throws IOException {
		if (tableManager == null) {
			synchronized (TableManager.class) {
				if (tableManager == null)
					tableManager = new TableManager(getExecutorService(), getTablesDirectory());
			}
		}
		return tableManager;
	}

	@Override
	public void close() {
		Tables.closeAll();
		super.close();
	}
}