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

import java.util.Optional;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to handle command line arguments for server configuration. */
public class CommandLineArgs {

  private static final Logger logger = LoggerFactory.getLogger(CommandLineArgs.class);

  private static final String CMD_LINE_ARG_PORT = "server.port";
  private static final String CMD_LINE_ARG_PORT_FILE_PATH = "port.file.path";
  private static final String CMD_LINE_ACCESS_LOGGING = "enable.access.logging";

  private static final Options OPTIONS =
      new Options()
          .addOption(
              Option.builder()
                  .longOpt(CMD_LINE_ARG_PORT)
                  .hasArg()
                  .desc(
                      "Caller-specified port for server to run on. If not specified, the app will"
                          + " choose a random open port and report the port number out to"
                          + CMD_LINE_ARG_PORT_FILE_PATH
                          + " if file path was specified.")
                  .build())
          .addOption(
              Option.builder()
                  .longOpt(CMD_LINE_ARG_PORT_FILE_PATH)
                  .hasArg()
                  .desc(
                      "File path to write the port server is running on. The file is expected to"
                          + " already exist.")
                  .build())
          .addOption(
              Option.builder()
                  .longOpt(CMD_LINE_ACCESS_LOGGING)
                  .hasArg(false)
                  .desc(
                      "Access logging is disabled by default. This option can be used to enable"
                          + " it.")
                  .build());
  ;

  private final Integer port;
  private final String portFilePath;
  private final boolean accessLoggingEnabled;

  public CommandLineArgs(Integer port, String portFilePath, boolean accessLoggingEnabled) {
    this.port = port;
    this.portFilePath = portFilePath;
    this.accessLoggingEnabled = accessLoggingEnabled;
  }

  public Optional<Integer> getPort() {
    return Optional.ofNullable(port);
  }

  public String getPortFilePath() {
    return portFilePath;
  }

  public boolean isAccessLoggingEnabled() {
    return accessLoggingEnabled;
  }

  public static CommandLineArgs fromArgs(String[] args) {
    Integer port = null;
    String portFilePath = null;
    boolean enableAccessLogging = false;

    try {
      CommandLine cmd = new DefaultParser().parse(OPTIONS, args);

      if (cmd.hasOption(CMD_LINE_ARG_PORT)) {
        String portArg = cmd.getOptionValue(CMD_LINE_ARG_PORT);
        try {
          port = Integer.parseInt(portArg);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid port number specified.", e);
        }
      }

      if (cmd.hasOption(CMD_LINE_ARG_PORT_FILE_PATH)) {
        portFilePath = cmd.getOptionValue(CMD_LINE_ARG_PORT_FILE_PATH);
        if (portFilePath.isEmpty()) {
          throw new IllegalArgumentException("Port file path cannot be empty.");
        }
      }

      if (cmd.hasOption(CMD_LINE_ACCESS_LOGGING)) {
        enableAccessLogging = true;
      }
    } catch (ParseException e) {
      throw new IllegalArgumentException("Failed to parse args", e);
    }

    logger.info("Command line args: port={}, portFilePath={}", port, portFilePath);
    return new CommandLineArgs(port, portFilePath, enableAccessLogging);
  }
}
