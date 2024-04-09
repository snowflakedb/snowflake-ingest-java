package net.snowflake.ingest;

import static net.snowflake.ingest.connection.RequestBuilder.CLIENT_NAME;
import static net.snowflake.ingest.connection.RequestBuilder.DEFAULT_VERSION;
import static net.snowflake.ingest.connection.RequestBuilder.JAVA_USER_AGENT;
import static net.snowflake.ingest.connection.RequestBuilder.OS_INFO_USER_AGENT_FORMAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URL;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import net.snowflake.ingest.connection.HistoryRangeResponse;
import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.IngestResponse;
import net.snowflake.ingest.utils.StagedFileWrapper;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Example ingest sdk integration test */
public class SimpleIngestIT {
  private final String TEST_FILE_NAME = "test1.csv";
  private final String TEST_FILE_NAME_2 = "test2.csv";
  private final String PIPE_NAME_PREFIX = "ingest_sdk_test_pipe_";

  private String testFilePath = null;
  private String testFilePath_2 = null;

  private String tableName = "";
  private String pipeName = "";
  private String pipeWithPatternName = "";
  private String stageName = "";
  private String stageWithPatternName = "";

  private final String PRODUCT_AND_PRODUCT_VERSION = CLIENT_NAME + "/" + DEFAULT_VERSION;

  // the object mapper we use for deserialization
  static ObjectMapper mapper = new ObjectMapper();

  private final Random RAND = new Random();

  private final Long RAND_NUM = Math.abs(RAND.nextLong());

  /** Create test table and pipe */
  @Before
  public void beforeAll() throws Exception {

    // get test file path

    URL resource = SimpleIngestIT.class.getResource(TEST_FILE_NAME);
    testFilePath = resource.getFile();
    resource = SimpleIngestIT.class.getResource(TEST_FILE_NAME_2);
    testFilePath_2 = resource.getFile();

    // create stage, pipe, and table

    tableName = "ingest_sdk_test_table_" + RAND_NUM;

    pipeName = PIPE_NAME_PREFIX + RAND_NUM;

    pipeWithPatternName = "ingest_sdk_test_pipe_pattern_" + RAND_NUM;

    stageName = "ingest_sdk_test_stage_" + RAND_NUM;

    stageWithPatternName = "ingest_sdk_test_stage_pattern" + RAND_NUM;

    String databaseName = TestUtils.getDatabase();

    String schemaName = TestUtils.getSchema();

    TestUtils.executeQuery("create database if not exists " + databaseName);

    TestUtils.executeQuery("create schema if not exists " + schemaName);

    TestUtils.executeQuery("use database " + databaseName);

    TestUtils.executeQuery("use schema " + schemaName);

    TestUtils.executeQuery("create or replace table " + tableName + " (str string, num int)");

    TestUtils.executeQuery("create or replace stage " + stageName);

    TestUtils.executeQuery("create or replace stage " + stageWithPatternName);

    createPipe(tableName, stageName, pipeName);

    TestUtils.executeQuery(
        "create or replace pipe "
            + pipeWithPatternName
            + " as copy into "
            + tableName
            + " from @"
            + stageWithPatternName
            + " pattern = 'test2*.csv'");
  }

  /** Remove test table and pipe */
  @After
  public void afterAll() {
    TestUtils.executeQuery("drop pipe if exists " + pipeName);

    TestUtils.executeQuery("drop pipe if exists " + pipeWithPatternName);

    TestUtils.executeQuery("drop stage if exists " + stageName);

    TestUtils.executeQuery("drop stage if exists " + stageWithPatternName);

    TestUtils.executeQuery("drop table if exists " + tableName);
  }

  /** ingest test example ingest a simple file and check load history. */
  @Test
  @Ignore // SNOW-957347: re-enable after fix
  public void testSimpleIngest() throws Exception {
    // put
    TestUtils.executeQuery("put file://" + testFilePath + " @" + stageName);

    // create ingest manager
    SimpleIngestManager manager = TestUtils.getManager(pipeName);

    testAndVerifySimpleIngestionForOnePipe(manager);
  }

  /** ingest test example ingest a simple file and check load history. */
  @Test
  public void testSimpleIngestWithPattern() throws Exception {
    // put
    TestUtils.executeQuery("put file://" + testFilePath + " @" + stageWithPatternName);

    TestUtils.executeQuery("put file://" + testFilePath_2 + " @" + stageWithPatternName);

    // create ingest manager
    SimpleIngestManager manager = TestUtils.getManager(pipeWithPatternName);
    Set<String> files = new HashSet<>();
    files.add(TEST_FILE_NAME);
    files.add(TEST_FILE_NAME_2);

    // get an insert response after we submit
    IngestResponse insertResponse =
        manager.ingestFiles(SimpleIngestManager.wrapFilepaths(files), null);

    assertEquals("SUCCESS", insertResponse.getResponseCode());
    assertEquals(1, insertResponse.getUnmatchedPatternFiles().size());
    assertEquals(
        TEST_FILE_NAME, insertResponse.getUnmatchedPatternFiles().stream().findFirst().get());

    // Get history and ensure that the expected file has been ingested
    getHistoryAndAssertLoad(manager, TEST_FILE_NAME_2);

    IngestResponse insertResponseSkippedFiles =
        manager.ingestFiles(
            SimpleIngestManager.wrapFilepaths(files),
            null /*Request Id*/,
            true /*Show Skipped Files*/);

    assertEquals("SUCCESS", insertResponseSkippedFiles.getResponseCode());
    assertEquals(1, insertResponseSkippedFiles.getSkippedFiles().size());
    assertEquals(
        TEST_FILE_NAME_2, insertResponseSkippedFiles.getSkippedFiles().stream().findFirst().get());
    assertEquals(1, insertResponseSkippedFiles.getUnmatchedPatternFiles().size());
    assertEquals(
        TEST_FILE_NAME,
        insertResponseSkippedFiles.getUnmatchedPatternFiles().stream().findFirst().get());
  }

  private void getHistoryAndAssertLoad(SimpleIngestManager manager, String test_file_name_2)
      throws InterruptedException, java.util.concurrent.ExecutionException,
          java.util.concurrent.TimeoutException {
    // keeps track of whether we've loaded the file
    boolean loaded = false;

    // create a new thread
    ExecutorService service = Executors.newSingleThreadExecutor();

    // fork off waiting for a load to the service
    Future<?> result =
        service.submit(
            () -> {
              String startTime = Instant.ofEpochMilli(System.currentTimeMillis()).toString();
              String beginMark = null;

              while (true) {

                try {
                  Thread.sleep(5000);

                  String endTime = Instant.ofEpochMilli(System.currentTimeMillis()).toString();
                  HistoryResponse response = manager.getHistory(null, null, beginMark);
                  HistoryRangeResponse rangeResponse =
                      manager.getHistoryRange(null, startTime, endTime);

                  assertEquals(response.files.size(), rangeResponse.files.size());

                  if (response != null && response.getNextBeginMark() != null) {
                    beginMark = response.getNextBeginMark();
                  }
                  if (response != null && response.files != null) {
                    for (HistoryResponse.FileEntry entry : response.files) {
                      // if we have a complete file that we've
                      // loaded with the same name..
                      String filename = entry.getPath();
                      if (entry.getPath() != null
                          && entry.isComplete()
                          && filename.equals(test_file_name_2)) {
                        return;
                      }
                    }
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                }
              }
            });

    // try to wait until the future is done
    try {
      // wait up to 3 minutes to load
      result.get(3, TimeUnit.MINUTES);
      loaded = true;
    } finally {
      assertTrue(loaded);
    }
  }

  /* This should be same for all three APIs since only SimpleIngestManager constructor has changed */
  @Test
  public void testUserAgentSuffixForInsertFileAPI() throws Exception {
    TestUtils.executeQuery("put file://" + testFilePath + " @" + stageName);

    final String userAgentSuffix = "kafka-provider/NONE";

    // create ingest manager
    SimpleIngestManager manager = TestUtils.getManager(pipeName, userAgentSuffix);

    // create a file wrapper
    StagedFileWrapper myFile = new StagedFileWrapper(TEST_FILE_NAME, null);

    HttpPost postWithAdditionUserAgentInfo =
        manager
            .getRequestBuilder()
            .generateInsertRequest(
                UUID.randomUUID(), pipeName, Collections.singletonList(myFile), false);

    verifyDefaultUserAgent(postWithAdditionUserAgentInfo.getAllHeaders(), true, userAgentSuffix);

    SimpleIngestManager testBuilderIngestManager =
        TestUtils.getManagerUsingBuilderPattern(pipeName, userAgentSuffix);
    HttpPost postWithAdditionUserAgentInfo2 =
        testBuilderIngestManager
            .getRequestBuilder()
            .generateInsertRequest(
                UUID.randomUUID(), pipeName, Collections.singletonList(myFile), false);

    verifyDefaultUserAgent(postWithAdditionUserAgentInfo2.getAllHeaders(), true, userAgentSuffix);

    // Passing null and empty string would also work
    // create ingest manager
    SimpleIngestManager nullUserAgentSuffixIngestManager = TestUtils.getManager(pipeName, null);

    HttpPost nullAdditionalUserAgent =
        nullUserAgentSuffixIngestManager
            .getRequestBuilder()
            .generateInsertRequest(
                UUID.randomUUID(), pipeName, Collections.singletonList(myFile), false);
    verifyDefaultUserAgent(nullAdditionalUserAgent.getAllHeaders(), false, null);

    SimpleIngestManager emptyUserAgentSuffixIngestManager = TestUtils.getManager(pipeName, null);
    HttpPost emptyAdditionalUserAgent =
        emptyUserAgentSuffixIngestManager
            .getRequestBuilder()
            .generateInsertRequest(
                UUID.randomUUID(), pipeName, Collections.singletonList(myFile), false);
    verifyDefaultUserAgent(emptyAdditionalUserAgent.getAllHeaders(), false, null);

    // Should return a default one. i.e if nothing is passed, it should be same as either null or
    // empty. But this check is for verifying backward compatibility
    SimpleIngestManager noUserAgentUsedIngestManager = TestUtils.getManager(pipeName);
    HttpPost noUserAgentUsed =
        noUserAgentUsedIngestManager
            .getRequestBuilder()
            .generateInsertRequest(
                UUID.randomUUID(), pipeName, Collections.singletonList(myFile), false);
    verifyDefaultUserAgent(noUserAgentUsed.getAllHeaders(), false, null);
  }

  /**
   * Creates multiple pipes with same stage and table to verify behavior of HttpUtil and BG thread
   * for ConnectionPoolingManager
   *
   * <p>Creates the thread only two times since the manager was closed only once.
   */
  @Test
  @Ignore // SNOW-957347: re-enable after fix
  public void testMultipleSimpleIngestManagers() throws Exception {
    // put
    TestUtils.executeQuery("put file://" + testFilePath + " @" + stageName);

    // create ingest manager
    SimpleIngestManager manager = TestUtils.getManager(pipeName);

    manager.close();

    final String pipeName2 = PIPE_NAME_PREFIX + "" + (RAND_NUM + 1);
    createPipe(tableName, stageName, pipeName2);

    SimpleIngestManager manager2 = TestUtils.getManager(pipeName2);

    testAndVerifySimpleIngestionForOnePipe(manager2);

    final String pipeName3 = PIPE_NAME_PREFIX + "" + (RAND_NUM + 2);
    createPipe(tableName, stageName, pipeName3);

    // creating one more SimpleIngestManager
    SimpleIngestManager manager3 = TestUtils.getManager(pipeName3);
    testAndVerifySimpleIngestionForOnePipe(manager3);
  }

  private static void createPipe(
      final String tableName, final String stageName, final String pipeName) throws Exception {
    TestUtils.executeQuery(
        "create or replace pipe "
            + pipeName
            + " as copy into "
            + tableName
            + " from @"
            + stageName);
  }

  private void testAndVerifySimpleIngestionForOnePipe(final SimpleIngestManager simpleIngestManager)
      throws Exception {
    // create a file wrapper
    StagedFileWrapper myFile = new StagedFileWrapper(TEST_FILE_NAME, null);

    // get an insert response after we submit
    IngestResponse insertResponse = simpleIngestManager.ingestFile(myFile, null);

    assertEquals("SUCCESS", insertResponse.getResponseCode());

    // Get history and ensure that the expected file has been ingested
    getHistoryAndAssertLoad(simpleIngestManager, TEST_FILE_NAME);

    IngestResponse insertResponseSkippedFiles = simpleIngestManager.ingestFile(myFile, null, true);

    assertEquals("SUCCESS", insertResponseSkippedFiles.getResponseCode());
    assertEquals(1, insertResponseSkippedFiles.getSkippedFiles().size());
    assertEquals(
        TEST_FILE_NAME, insertResponseSkippedFiles.getSkippedFiles().stream().findFirst().get());
  }

  private void verifyDefaultUserAgent(
      final Header[] headers,
      final boolean verifyAdditionalUserAgentInfo,
      final String httpUserAgentInformation) {
    for (Header h : headers) {
      if (h.getName().equalsIgnoreCase(HttpHeaders.USER_AGENT)) {
        System.out.println(h);
        if (verifyAdditionalUserAgentInfo) {
          assertTrue(h.getValue().contains(httpUserAgentInformation));
          assertTrue(h.getValue().endsWith(httpUserAgentInformation));
        }

        // This should always be present
        assertTrue(h.getValue().contains(PRODUCT_AND_PRODUCT_VERSION));
        String javaAndVersion = JAVA_USER_AGENT + "/" + System.getProperty("java.version");
        assertTrue(h.getValue().contains(javaAndVersion));
        final String osInformation =
            String.format(
                OS_INFO_USER_AGENT_FORMAT,
                System.getProperty("os.name"),
                System.getProperty("os.version"),
                System.getProperty("os.arch"));
        assertTrue(h.getValue().contains(osInformation));
      }
    }
  }
}
