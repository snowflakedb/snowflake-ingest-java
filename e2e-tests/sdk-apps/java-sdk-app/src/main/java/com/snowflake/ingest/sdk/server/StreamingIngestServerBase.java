package com.snowflake.ingest.sdk.server;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.docs.DocService;
import com.linecorp.armeria.server.logging.AccessLogWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Armeria servers that provides common functionality. Used by {@link
 * StreamingIngestJavaServer} and {@link StreamingIngestJavaServerForE2E}
 */
public class StreamingIngestServerBase {

  private static final Logger logger = LoggerFactory.getLogger(StreamingIngestServerBase.class);
  private static final String DOCS_ENDPOINT = "/docs";

  public static ServerBuilder buildServer(CommandLineArgs commandLineArgs) {
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

  public static void startServer(ServerBuilder serverBuilder, String portFilePath)
      throws IOException {
    Server server = serverBuilder.build();
    server.closeOnJvmShutdown();
    server.start().join();

    // Write the port to a file if the file path is provided.
    writePortToFile(portFilePath, server.activeLocalPort());

    logger.info(
        "Java StreamingIngestServer started successfully on port: {}", server.activeLocalPort());
  }

  /**
   * Writes the port number to a file.
   *
   * @param filePath the file path to write the port to
   */
  static void writePortToFile(String filePath, int port) throws IOException {
    if (filePath == null) {
      logger.info("Port file path not provided. Not writing port to file.");
      return;
    }

    File file = new File(filePath);
    if (!file.exists()) {
      throw new IllegalArgumentException("Port file doesn't exist. File path: " + filePath);
    }

    Files.write(Paths.get(filePath), String.valueOf(port).getBytes());
    logger.info("Port {} written to file: {}", port, filePath);
  }
}
