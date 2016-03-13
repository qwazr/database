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
 */
package com.qwazr.database;

import com.qwazr.cluster.manager.ClusterManager;
import com.qwazr.utils.server.AbstractServer;
import com.qwazr.utils.server.ServerConfiguration;
import com.qwazr.utils.server.ServiceInterface;
import com.qwazr.utils.server.ServletApplication;
import io.undertow.security.idm.IdentityManager;

import javax.servlet.ServletException;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Executors;

public class TableServer extends AbstractServer<ServerConfiguration> {

	private TableServer() {
		super(Executors.newSingleThreadExecutor(), new ServerConfiguration());
	}

	@Override
	public ServletApplication load(Collection<Class<? extends ServiceInterface>> services) throws IOException {
		File currentDataDir = getCurrentDataDir();
		services.add(ClusterManager.load(executorService, getWebServicePublicAddress(), null));
		services.add(TableManager.load(executorService, currentDataDir));
		return null;
	}

	@Override
	protected IdentityManager getIdentityManager(String realm) {
		return null;
	}

	public static void main(String[] args)
			throws IOException, ServletException, InstantiationException, IllegalAccessException {
		new TableServer().start(true);
	}

}