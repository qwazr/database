/**
 * Copyright 2015 Emmanuel Keller / QWAZR
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
package com.qwazr.database;

import com.qwazr.cluster.ClusterServer;
import com.qwazr.cluster.manager.ClusterManager;
import com.qwazr.utils.server.AbstractServer;
import com.qwazr.utils.server.RestApplication;
import com.qwazr.utils.server.ServerException;
import com.qwazr.utils.server.ServletApplication;
import io.undertow.security.idm.IdentityManager;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import javax.servlet.ServletException;
import javax.ws.rs.ApplicationPath;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Set;

public class TableServer extends AbstractServer {

	public final static String SERVICE_NAME_TABLE = "table";

	private final static ServerDefinition serverDefinition = new ServerDefinition();

	static {
		serverDefinition.defaultWebApplicationTcpPort = 9093;
		serverDefinition.mainJarPath = "qwazr-database.jar";
		serverDefinition.defaultDataDirName = "database";
	}

	private TableServer() {
		super(serverDefinition);
	}

	@ApplicationPath("/")
	public static class GraphApplication extends RestApplication {

		@Override
		public Set<Class<?>> getClasses() {
			Set<Class<?>> classes = super.getClasses();
			classes.add(TableServiceImpl.class);
			return classes;
		}
	}

	@Override
	public void commandLine(CommandLine cmd) throws IOException, ParseException {
	}

	@Override
	protected Class<GraphApplication> getRestApplication() {
		return GraphApplication.class;
	}

	@Override
	protected Class<ServletApplication> getServletApplication() {
		return null;
	}

	@Override
	protected IdentityManager getIdentityManager(String realm) {
		return null;
	}

	public static void load(File dataDir) throws IOException {
		try {
			File tableDir = new File(dataDir, SERVICE_NAME_TABLE);
			if (!tableDir.exists())
				tableDir.mkdir();
			TableManager.load(tableDir);
		} catch (URISyntaxException | ServerException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void load() throws IOException {
		File dataDir = getCurrentDataDir();
		ClusterServer.load(getWebServicePublicAddress(), dataDir);
		load(dataDir);
	}

	public static void main(String[] args) throws IOException, ParseException, ServletException, InstantiationException,
					IllegalAccessException {
		new TableServer().start(args);
		ClusterManager.INSTANCE.registerMe(SERVICE_NAME_TABLE);
	}

}
