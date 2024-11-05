/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.SubscopedTokenExternalVolumeManager.generateBlobPathFromLocationInfoPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.Utils;
import org.apache.commons.lang3.function.TriConsumer;
import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SubscopedTokenExternalVolumeManagerTest {
  private SubscopedTokenExternalVolumeManager manager;
  private ExecutorService executorService;

  @Before
  public void setup() {
    this.manager =
        new SubscopedTokenExternalVolumeManager(
            "role",
            "clientName",
            MockSnowflakeServiceClient.create(true /* enableIcebergStreaming */));
  }

  @After
  public void teardown() {
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  @Test
  public void testRegister() {
    assertThatCode(() -> this.manager.registerTable(new TableRef("db", "schema", "table")))
        .doesNotThrowAnyException();
  }

  @Test
  public void testConcurrentRegisterTable() throws Exception {
    int numThreads = 50;
    int timeoutInSeconds = 30;
    List<Future<InternalStage>> allResults =
        doConcurrentTest(
            numThreads,
            timeoutInSeconds,
            () -> manager.registerTable(new TableRef("db", "schema", "table")),
            () -> manager.getStorage("db.schema.table"));
    InternalStage extvol = manager.getStorage("db.schema.table");
    assertThat(extvol).isNotNull();
    for (int i = 0; i < numThreads; i++) {
      assertThat(allResults.get(i).get(timeoutInSeconds, TimeUnit.SECONDS))
          .as("getStorage from threadId=%d", i)
          .isSameAs(extvol);
    }
  }

  @Test
  public void testGetStorage() {
    this.manager.registerTable(new TableRef("db", "schema", "table"));
    InternalStage extvol = this.manager.getStorage("db.schema.table");
    assertThat(extvol).isNotNull();
  }

  @Test
  public void testGetStorageWithoutRegister() {
    assertThatExceptionOfType(SFException.class)
        .isThrownBy(() -> manager.getStorage("db.schema.table"))
        .withMessageEndingWith("No external volume found for tableRef=db.schema.table.")
        .has(
            new Condition<SFException>(
                ex -> ex.getVendorCode().equals("0001"), "vendor code 0001"));
  }

  @Test
  public void testGenerateBlobPathPublic() {
    manager.registerTable(new TableRef("db", "schema", "table"));
    BlobPath blobPath = manager.generateBlobPath("db.schema.table");
    assertThat(blobPath).isNotNull();
    assertThat(blobPath.uploadPath).isNotNull().endsWith("/snow_volHash_figsId_1_1_0.parquet");

    assertThat(blobPath.fileRegistrationPath)
        .isNotNull()
        .isEqualTo("table/data/streaming_ingest/figsId/" + blobPath.uploadPath);
  }

  @Test
  public void testGenerateBlobPathInternals() {
    int i = 1;
    assertThatThrownBy(() -> generateBlobPathFromLocationInfoPath("db.sch.tbl1", "snow_", "0a", 1))
        .hasMessageEndingWith("File path returned by server is invalid.");
    assertThatThrownBy(
            () -> generateBlobPathFromLocationInfoPath("db.sch.tbl1", "1/snow_", "0a", 1))
        .hasMessageEndingWith("File path returned by server is invalid.");
    assertThatThrownBy(
            () -> generateBlobPathFromLocationInfoPath("db.sch.tbl1", "1/2/snow_", "0a", 1))
        .hasMessageEndingWith("File path returned by server is invalid.");

    AtomicInteger counter = new AtomicInteger(1);
    TriConsumer<String, String, String> test =
        (String testPath, String pathPrefix, String fileName) -> {
          String twoHexChars = Utils.getTwoHexChars();
          BlobPath blobPath =
              generateBlobPathFromLocationInfoPath(
                  "db.sch.tbl1", testPath, twoHexChars, counter.getAndIncrement());
          assertThat(blobPath).isNotNull();
          assertThat(blobPath.uploadPath)
              .isNotNull()
              .isEqualTo(String.format("%s/%s", twoHexChars, fileName));
          assertThat(blobPath.fileRegistrationPath)
              .isNotNull()
              .isEqualTo(String.format("%s/%s/%s", pathPrefix, twoHexChars, fileName));
        };

    // happypath
    test.accept(
        "vol/table/data/streaming_ingest/figsId/snow_",
        "vol/table/data/streaming_ingest/figsId",
        "snow_1.parquet");

    // vol has extra subfoldrs
    test.accept(
        "vol/vol2/vol3/table/data/streaming_ingest/figsId/snow_",
        "vol/vol2/vol3/table/data/streaming_ingest/figsId",
        "snow_2.parquet");

    // table has extra subfolders
    test.accept(
        "vol/table/t2/t3/data/streaming_ingest/figsId/snow_",
        "vol/table/t2/t3/data/streaming_ingest/figsId",
        "snow_3.parquet");

    // no table
    test.accept(
        "vol/data/streaming_ingest/figsId/snow_",
        "vol/data/streaming_ingest/figsId",
        "snow_4.parquet");
  }

  @Test
  public void testConcurrentGenerateBlobPath() throws Exception {
    int numThreads = 50;
    int timeoutInSeconds = 60;
    manager.registerTable(new TableRef("db", "schema", "table"));

    List<Future<BlobPath>> allResults =
        doConcurrentTest(
            numThreads,
            timeoutInSeconds,
            () -> {
              for (int i = 0; i < 1000; i++) {
                manager.generateBlobPath("db.schema.table");
              }
            },
            () -> manager.generateBlobPath("db.schema.table"));
    for (int i = 0; i < numThreads; i++) {
      BlobPath blobPath = allResults.get(0).get(timeoutInSeconds, TimeUnit.SECONDS);
      assertThat(blobPath).isNotNull();
      assertThat(blobPath.uploadPath).contains("/snow_volHash_figsId_1_1_");
    }
  }

  private <T> List<Future<T>> doConcurrentTest(
      int numThreads, int timeoutInSeconds, Runnable action, Supplier<T> getResult)
      throws Exception {
    assertThat(executorService).isNull();

    executorService =
        new ThreadPoolExecutor(
            numThreads, numThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
    List<Callable<T>> tasks = new ArrayList<>();
    final CyclicBarrier startBarrier = new CyclicBarrier(numThreads);
    final CyclicBarrier endBarrier = new CyclicBarrier(numThreads);
    for (int i = 0; i < numThreads; i++) {
      tasks.add(
          () -> {
            startBarrier.await(timeoutInSeconds, TimeUnit.SECONDS);
            action.run();
            endBarrier.await();
            return getResult.get();
          });
    }

    List<Future<T>> allResults = executorService.invokeAll(tasks);
    allResults.get(0).get(timeoutInSeconds, TimeUnit.SECONDS);
    return allResults;
  }

  @Test
  public void testGetClientPrefix() {
    assertThat(manager.getClientPrefix()).isEqualTo("test_prefix_123");
  }
}
