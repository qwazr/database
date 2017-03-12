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

import com.qwazr.cluster.ClusterManager;
import com.qwazr.server.BaseServer;
import com.qwazr.server.GenericServer;
import com.qwazr.server.WelcomeShutdownService;
import com.qwazr.server.configuration.ServerConfiguration;

import javax.management.MBeanException;
import javax.management.OperationsException;
import javax.servlet.ServletException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TableServer implements BaseServer {

	private final GenericServer server;
	private final TableManager tableManager;
	private final ClusterManager clusterManager;
	private final TableServiceBuilder serviceBuilder;

	public TableServer(final ServerConfiguration serverConfiguration) throws IOException, URISyntaxException {
		final ExecutorService executorService = Executors.newCachedThreadPool();
		final GenericServer.Builder builder =
				GenericServer.of(serverConfiguration, executorService).webService(WelcomeShutdownService.class);
		clusterManager =
				new ClusterManager(executorService, serverConfiguration).registerHttpClientMonitoringThread(builder)
						.registerProtocolListener(builder)
						.registerWebService(builder);
		tableManager = new TableManager(
				TableManager.checkTablesDirectory(serverConfiguration.dataDirectory.toPath())).registerWebService(
				builder).registerShutdownListener(builder);
		serviceBuilder = new TableServiceBuilder(clusterManager, tableManager);
		server = builder.build();
	}

	public TableServiceBuilder getServiceBuilder() {
		return serviceBuilder;
	}

	public GenericServer getServer() {
		return server;
	}

	private static volatile TableServer INSTANCE;

	public static synchronized TableServer getInstance() {
		return INSTANCE;
	}

	public static synchronized void main(final String... args)
			throws IOException, ReflectiveOperationException, OperationsException, ServletException, MBeanException,
			URISyntaxException, InterruptedException {
		if (INSTANCE != null)
			shutdown();
		INSTANCE = new TableServer(new ServerConfiguration(args));
		INSTANCE.start();
	}

	public static synchronized void shutdown() {
		if (INSTANCE != null)
			INSTANCE.stop();
		INSTANCE = null;
	}

}