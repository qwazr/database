/*
 * Copyright 2015-2017 Emmanuel Keller / QWAZR
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
import com.qwazr.cluster.ClusterServiceInterface;
import com.qwazr.server.ApplicationBuilder;
import com.qwazr.server.BaseServer;
import com.qwazr.server.GenericServer;
import com.qwazr.server.GenericServerBuilder;
import com.qwazr.server.RestApplication;
import com.qwazr.server.WelcomeShutdownService;
import com.qwazr.server.configuration.ServerConfiguration;

import javax.management.JMException;
import javax.servlet.ServletException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class TableServer implements BaseServer {

	private final GenericServer server;
	private final TableServiceBuilder serviceBuilder;

	public TableServer(final ServerConfiguration serverConfiguration) throws IOException, URISyntaxException {

		final TableSingleton tableSingleton = new TableSingleton(serverConfiguration.dataDirectory.toPath(), null);
		final ExecutorService executorService = tableSingleton.getExecutorService();

		final GenericServerBuilder builder = GenericServer.of(serverConfiguration, executorService);
		final ApplicationBuilder webServices = ApplicationBuilder.of("/*").classes(RestApplication.JSON_CLASSES).
				singletons(new WelcomeShutdownService());
		final Set<String> services = new HashSet<>();
		services.add(ClusterServiceInterface.SERVICE_NAME);
		services.add(TableServiceInterface.SERVICE_NAME);

		final ClusterManager clusterManager =
				new ClusterManager(executorService, serverConfiguration).registerProtocolListener(builder, services)
						.registerWebService(webServices);
		final TableManager tableManager = tableSingleton.getTableManager();
		webServices.singletons(tableManager.getService());
		serviceBuilder = new TableServiceBuilder(clusterManager, tableManager);
		builder.getWebServiceContext().jaxrs(webServices);
		builder.shutdownListener(server -> tableSingleton.close());
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
			throws IOException, ReflectiveOperationException, ServletException, JMException, URISyntaxException {
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