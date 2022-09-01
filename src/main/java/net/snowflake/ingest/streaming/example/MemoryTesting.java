/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.example;

import static net.snowflake.ingest.utils.Utils.showMemory;

import java.util.ArrayList;
import java.util.List;
import net.snowflake.ingest.utils.Utils;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example on how to use the Streaming Ingest client APIs.
 *
 * <p>Please read the README.md file for detailed steps
 */
public class MemoryTesting {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryTesting.class);

  private static String toMB(long init) {
    return (Long.valueOf(init).doubleValue() / (1024 * 1024)) + " MB";
  }

  private static void channelBufferManual() throws Exception {
    // LOG.info("PID: {}", ProcessHandle.current().pid());
    showMemory();
    // LOG.info("Making direct buffer available");
    // MemoryUtil.UNSAFE.addressSize();
    showMemory();

    RootAllocator rootAllocator = new RootAllocator();
    LOG.info("Allocating buffer");
    ArrowBuf buffer = rootAllocator.buffer(100 * 1024 * 1024);
    LOG.info("Buffer allocated");
    showMemory();

    LOG.info("Closing");
    buffer.close();
    Utils.closeAllocator(rootAllocator);
    LOG.info("After close");
    showMemory();
  }

  private static void channelBufferManual2() throws Exception {
    // LOG.info("PID: {}", ProcessHandle.current().pid());
    showMemory();
    LOG.info("Making direct buffer available");
    MemoryUtil.UNSAFE.addressSize();
    showMemory();

    RootAllocator rootAllocator = new RootAllocator();
    LOG.info("Allocating buffer");
    ArrowType type = Types.MinorType.INT.getType();
    FieldType fieldType = new FieldType(true, type, null, null);
    Field field = new Field("int", fieldType, null);
    FieldVector vector = field.createVector(rootAllocator);
    List<FieldVector> vectors = new ArrayList<>();
    vectors.add(vector);
    VectorSchemaRoot root = new VectorSchemaRoot(vectors);

    ((IntVector) vector).setSafe(0, 1);

    showMemory();

    root.close();
    Utils.closeAllocator(rootAllocator);

    LOG.info("Buffer allocated");
    showMemory();

    int max = 100;
    for (int i = 0; i < 20; i++) {
      showMemory();
      System.gc();
    }
  }

  //  private static void singleChannelUsage() throws Exception {
  //    LOG.info("PID: {}", ProcessHandle.current().pid());
  //    showMemory();
  //
  //    StreamingIngest streamingIngest = streamingIngest();
  //    RandomDataSapClient randomDataSapClient = new RandomDataSapClient(10, 11, 1_000_000,
  // 1_000_001);
  //
  //    LOG.info("Starting processing");
  //
  //    final Iterable<Map<String, Object>> rows = generateRows(randomDataSapClient);
  //    Consumer<StreamingIngest.Channel> job =
  //        (channel) -> {
  //          channel.insertRows(rows);
  //          try {
  //            showMemory();
  //            Thread.sleep(100);
  //          } catch (Exception e) {
  //            throw new RuntimeException(e);
  //          }
  //        };
  //
  //    LOG.info("Opening channels");
  //    StreamingIngest.Channel channel =
  //        streamingIngest.openChannel("SAP_ACHYZY", "INTERNAL_STUB", "FAKE_TABLE_3");
  //
  //    LOG.info("Channel usage");
  //    job.accept(channel);
  //    Thread.sleep(10_000);
  //    showMemory();
  //
  //    LOG.info("Finished processing");
  //    LOG.info("Closing channel");
  //    channel.close().join();
  //    streamingIngest.close();
  //
  //    LOG.info("All done");
  //    showMemory();
  //  }
  //
  //  private StreamingIngest streamingIngest() throws IOException {
  //    val properties = new Properties();
  //    properties.put(Constants.ACCOUNT_URL, "ZG16045.eu-west-1.snowflakecomputing.com");
  //    properties.put(Constants.USER, "ACHYZY");
  //    properties.put(Constants.ROLE, "SAP_ACHYZY_AGENT_ROLE");
  //    properties.put(
  //            Constants.PRIVATE_KEY,
  //            Files.readString(
  //                    Path.of(
  //
  // "/Users/achyzy/Projects/connectors/Connectors/SAP/agent/agent/config/rsa_key.p8")));
  //
  //    SnowflakeStreamingIngestClient client =
  //            SnowflakeStreamingIngestClientFactory.builder("SAP_CONNECTOR_AGENT")
  //                    .setProperties(properties)
  ////
  // .setParameterOverrides(Map.ofEntries(entry("STREAMING_INGEST_CLIENT_SDK_BUFFER_FLUSH_INTERVAL_IN_MILLIS", 1_000)))
  //                    .build();
  //    StreamingIngest streamingIngest = new StreamingIngest(client);
  //    return streamingIngest;
  //  }

  public static void main(String[] args) throws Exception {
    channelBufferManual2();
  }
}
