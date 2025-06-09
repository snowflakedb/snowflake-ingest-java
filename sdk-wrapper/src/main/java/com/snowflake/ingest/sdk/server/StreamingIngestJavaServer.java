/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
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

package com.snowflake.ingest.sdk.server;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.docs.DocService;
import com.linecorp.armeria.server.logging.AccessLogWriter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streaming Ingest Server that provides a REST API wrapper around the Snowflake Streaming Ingest
 * SDK. The server can be started on a specified port or a random available port.
 */
public class StreamingIngestJavaServer {
  private static final Logger logger = LoggerFactory.getLogger(StreamingIngestJavaServer.class);
  private static final String DOCS_ENDPOINT = "/docs";

  public static void main(String[] args) throws IOException {
    CommandLineArgs commandLineArgs = CommandLineArgs.fromArgs(args);
    ServerBuilder serverBuilder = buildServer(commandLineArgs);
    startServer(serverBuilder);
  }

  private static ServerBuilder buildServer(CommandLineArgs commandLineArgs) {
    ServerBuilder serverBuilder =
        Server.builder()
            .http(
                commandLineArgs
                    .getPort()
                    .orElse(0)) // Choose a random open port if not specified by the user.
            .service("/", (ctx, req) -> HttpResponse.ofRedirect(DOCS_ENDPOINT))
            .serviceUnder(DOCS_ENDPOINT, DocService.builder().build())
            .annotatedService(new StreamingIngestJavaService())
            .maxRequestLength(0); // Disable request length limit

    if (commandLineArgs.isAccessLoggingEnabled()) {
      logger.info("Access logging enabled");
      serverBuilder.accessLogWriter(AccessLogWriter.combined(), true);
    } else {
      logger.info("Access logging disabled");
    }

    return serverBuilder;
  }

  private static void startServer(ServerBuilder serverBuilder) throws IOException {
    Server server = serverBuilder.build();
    server.closeOnJvmShutdown();
    server.start().join();

    logger.info(
        "Java StreamingIngestServer started successfully on port: {}", server.activeLocalPort());
  }
}
