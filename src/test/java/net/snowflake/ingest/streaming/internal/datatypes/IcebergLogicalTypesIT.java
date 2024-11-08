/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.datatypes;

import java.util.Arrays;
import net.snowflake.ingest.IcebergIT;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(IcebergIT.class)
@RunWith(Parameterized.class)
public class IcebergLogicalTypesIT extends AbstractDataTypeTest {
  @Parameterized.Parameters(name = "compressionAlgorithm={0}, icebergSerializationPolicy={1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"ZSTD", Constants.IcebergSerializationPolicy.COMPATIBLE},
      {"ZSTD", Constants.IcebergSerializationPolicy.OPTIMIZED}
    };
  }

  @Parameterized.Parameter(0)
  public static String compressionAlgorithm;

  @Parameterized.Parameter(1)
  public static Constants.IcebergSerializationPolicy icebergSerializationPolicy;

  @Before
  public void before() throws Exception {
    super.setUp(true, compressionAlgorithm, icebergSerializationPolicy);
  }

  @Test
  public void testBoolean() throws Exception {
    testIcebergIngestion("boolean", true, new BooleanProvider());
    testIcebergIngestion("boolean", false, new BooleanProvider());
    testIcebergIngestion("boolean", 1, true, new BooleanProvider());
    testIcebergIngestion("boolean", 0, false, new BooleanProvider());
    testIcebergIngestion("boolean", "1", true, new BooleanProvider());
    testIcebergIngestion("boolean", "0", false, new BooleanProvider());
    testIcebergIngestion("boolean", "true", true, new BooleanProvider());
    testIcebergIngestion("boolean", "false", false, new BooleanProvider());
    testIcebergIngestion("boolean", null, new BooleanProvider());

    SFException ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("boolean", new Object(), true, new BooleanProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("boolean not null", null, new BooleanProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
  }

  @Test
  public void testBooleanQueries() throws Exception {
    testIcebergIngestAndQuery(
        "boolean",
        Arrays.asList(true, false, false, true, false),
        "select {columnName} from {tableName}",
        Arrays.asList(true, false, false, true, false));
    testIcebergIngestAndQuery(
        "boolean",
        Arrays.asList(1, "0", 1, false, 1),
        "select {columnName} from {tableName}",
        Arrays.asList(true, false, true, false, true));
    testIcebergIngestAndQuery(
        "boolean",
        Arrays.asList("true", 1, null, "1", true),
        "select MIN({columnName}) from {tableName}",
        Arrays.asList(true));
    testIcebergIngestAndQuery(
        "boolean",
        Arrays.asList(null, null, null, "true", false),
        "select COUNT(*) from {tableName} where {columnName} is null",
        Arrays.asList(3L));
    testIcebergIngestAndQuery(
        "boolean",
        Arrays.asList(null, null, null),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList((Object) null));
  }

  @Test
  public void testBinary() throws Exception {
    testIcebergIngestion("binary", new byte[] {1, 2, 3}, new ByteArrayProvider());
    testIcebergIngestion("binary", "313233", new byte[] {49, 50, 51}, new ByteArrayProvider());
    testIcebergIngestion("binary", new byte[8388608], new ByteArrayProvider());
    testIcebergIngestion("binary", null, new ByteArrayProvider());

    SFException ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("binary", new byte[8388608 + 1], new ByteArrayProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_VALUE_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "binary", new Object(), new byte[] {1, 2, 3, 4, 5}, new ByteArrayProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("binary not null", null, new ByteArrayProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
  }

  @Test
  public void testBinaryAndQueries() throws Exception {
    testIcebergIngestAndQuery(
        "binary",
        Arrays.asList(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, new byte[] {7, 8, 9}),
        "select {columnName} from {tableName}",
        Arrays.asList(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, new byte[] {7, 8, 9}));
    testIcebergIngestAndQuery(
        "binary",
        Arrays.asList("313233", new byte[] {4, 5, 6}, "373839"),
        "select {columnName} from {tableName}",
        Arrays.asList(new byte[] {49, 50, 51}, new byte[] {4, 5, 6}, new byte[] {55, 56, 57}));
    testIcebergIngestAndQuery(
        "binary",
        Arrays.asList(new byte[8388608], new byte[8388608], new byte[8388608]),
        "select {columnName} from {tableName}",
        Arrays.asList(new byte[8388608], new byte[8388608], new byte[8388608]));
    testIcebergIngestAndQuery(
        "binary",
        Arrays.asList(null, null, null),
        "select {columnName} from {tableName}",
        Arrays.asList(null, null, null));
    testIcebergIngestAndQuery(
        "binary",
        Arrays.asList(null, new byte[] {1, 2, 3}, null),
        "select COUNT(*) from {tableName} where {columnName} is null",
        Arrays.asList(2L));
    byte[] max = new byte[8388608];
    Arrays.fill(max, (byte) 0xFF);
    testIcebergIngestAndQuery(
        "binary",
        Arrays.asList(new byte[8388608], max),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList(max));
    testIcebergIngestAndQuery(
        "binary",
        Arrays.asList(null, null, null),
        "select MIN({columnName}) from {tableName}",
        Arrays.asList((Object) null));
  }

  @Test
  public void testFixedLenByteArray() throws Exception {
    testIcebergIngestion("fixed(3)", new byte[] {1, 2, 3}, new ByteArrayProvider());
    testIcebergIngestion("fixed(3)", "313233", new byte[] {49, 50, 51}, new ByteArrayProvider());
    testIcebergIngestion("fixed(8388608)", new byte[8388608], new ByteArrayProvider());
    testIcebergIngestion("fixed(3)", null, new ByteArrayProvider());

    SFException ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "fixed(10)",
                    "313233",
                    new byte[] {49, 50, 51, 0, 0, 0, 0, 0, 0, 0},
                    new ByteArrayProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_VALUE_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "fixed(3)", new byte[] {49, 50, 51, 52}, new ByteArrayProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_VALUE_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "fixed(3)", "313", new byte[] {49, 50}, new ByteArrayProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_VALUE_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () ->
                testIcebergIngestion(
                    "fixed(3)", new Object(), new byte[] {1, 2, 3, 4, 5}, new ByteArrayProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());

    ex =
        Assertions.catchThrowableOfType(
            SFException.class,
            () -> testIcebergIngestion("fixed(3) not null", null, new ByteArrayProvider()));
    Assertions.assertThat(ex)
        .extracting(SFException::getVendorCode)
        .isEqualTo(ErrorCode.INVALID_FORMAT_ROW.getMessageCode());
  }

  @Test
  public void testFixedLenByteArrayAndQueries() throws Exception {
    testIcebergIngestAndQuery(
        "fixed(3)",
        Arrays.asList(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, new byte[] {7, 8, 9}),
        "select {columnName} from {tableName}",
        Arrays.asList(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}, new byte[] {7, 8, 9}));
    testIcebergIngestAndQuery(
        "fixed(3)",
        Arrays.asList("313233", new byte[] {4, 5, 6}, "373839"),
        "select {columnName} from {tableName}",
        Arrays.asList(new byte[] {49, 50, 51}, new byte[] {4, 5, 6}, new byte[] {55, 56, 57}));
    testIcebergIngestAndQuery(
        "fixed(8388608)",
        Arrays.asList(new byte[8388608], new byte[8388608], new byte[8388608]),
        "select {columnName} from {tableName}",
        Arrays.asList(new byte[8388608], new byte[8388608], new byte[8388608]));
    testIcebergIngestAndQuery(
        "fixed(3)",
        Arrays.asList(null, null, null),
        "select {columnName} from {tableName}",
        Arrays.asList(null, null, null));
    testIcebergIngestAndQuery(
        "fixed(3)",
        Arrays.asList(null, new byte[] {1, 2, 3}, null),
        "select COUNT(*) from {tableName} where {columnName} is null",
        Arrays.asList(2L));
    byte[] max = new byte[8388608];
    Arrays.fill(max, (byte) 0xFF);
    testIcebergIngestAndQuery(
        "fixed(8388608)",
        Arrays.asList(new byte[8388608], max),
        "select MAX({columnName}) from {tableName}",
        Arrays.asList(max));
    testIcebergIngestAndQuery(
        "fixed(3)",
        Arrays.asList(null, null, null),
        "select MIN({columnName}) from {tableName}",
        Arrays.asList((Object) null));
  }
}
