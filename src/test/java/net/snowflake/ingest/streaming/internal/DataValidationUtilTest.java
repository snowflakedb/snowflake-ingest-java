package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.DataValidationUtil.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.function.Function;
import javax.xml.bind.DatatypeConverter;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Test;

public class DataValidationUtilTest {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private void expectError(ErrorCode expectedErrorCode, Function func, Object args) {
    expectError(expectedErrorCode, () -> func.apply(args));
  }

  private void expectError(ErrorCode expectedErrorCode, Runnable action) {
    try {
      action.run();
      Assert.fail("Expected Exception");
    } catch (SFException e) {
      assertEquals(expectedErrorCode.getMessageCode(), e.getVendorCode());
    } catch (Exception e) {
      Assert.fail("Invalid error through");
    }
  }

  @Test
  public void testValidateAndParseTime() {
    assertEquals(5L, validateAndParseTime("00:00:05", 0).longValueExact());
    assertEquals(5000L, validateAndParseTime("00:00:05", 3).longValueExact());
    assertEquals(5000L, validateAndParseTime("00:00:05.000", 3).longValueExact());
    assertEquals(5123L, validateAndParseTime("00:00:05.123", 3).longValueExact());
    assertEquals(5123L, validateAndParseTime("00:00:05.123456", 3).longValueExact());
    assertEquals(5123456789L, validateAndParseTime("00:00:05.123456789", 9).longValueExact());

    assertEquals(72L, validateAndParseTime("72", 0).longValueExact());
    assertEquals(72000L, validateAndParseTime("72", 3).longValueExact());

    // Timestamps are rejected
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2/18/2008 02:36:48", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57:01", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57:01 +07:00", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57:01 +0700", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57:01-07", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57:01-07:00", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57:01.123456", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("2013-04-28 20:57:01.123456789 +07:00", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("2013-04-28 20:57:01.123456789 +0700", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57:01.123456789+07", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("2013-04-28 20:57:01.123456789+07:00", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28 20:57+07:00", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28T20", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28T20:57", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28T20:57:01", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28T20:57:01-07:00", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28T20:57:01.123456", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("2013-04-28T20:57:01.123456789+07:00", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28T20:57+07:00", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("Mon Jul 08 18:09:51 +0000 2013", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("Thu, 21 Dec 2000 04:01:07 PM", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("Thu, 21 Dec 2000 04:01:07 PM +0200", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("Thu, 21 Dec 2000 04:01:07.123456789 PM", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("Thu, 21 Dec 2000 04:01:07.123456789 PM +0200", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("Thu, 21 Dec 2000 16:01:07", 9));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime("Thu, 21 Dec 2000 16:01:07 +0200", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("Thu, 21 Dec 2000 16:01:07.123456789", 9));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime("Thu, 21 Dec 2000 16:01:07.123456789 +0200", 9));

    // Dates are rejected
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("2013-04-28", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("17-DEC-1980", 9));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("12/17/1980", 9));

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(LocalDate.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(LocalDateTime.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(OffsetDateTime.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(ZonedDateTime.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(new Date(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(1.5f, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(1.5, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("1.5", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("1.0", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(new Object(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(false, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime("foo", 3));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime(java.sql.Time.valueOf("20:57:00"), 3));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTime(java.sql.Date.valueOf("2010-11-03"), 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTime(java.sql.Timestamp.valueOf("2010-11-03 20:57:00"), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(BigInteger.ZERO, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime(BigDecimal.ZERO, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTime('c', 3));
  }

  @Test
  public void testValidateAndParseTimestampNtzSb16() {
    assertEquals(
        new TimestampWrapper(1609462800, 123000000, new BigInteger("1609462800123000000")),
        DataValidationUtil.validateAndParseTimestampNtzSb16("2021-01-01 01:00:00.123", 9, true));

    // Time formats are not supported
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("20:57:01.123456789+07:00", 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("20:57:01.123456789", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("20:57:01", 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("20:57", 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16("07:57:01.123456789 AM", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("04:01:07 AM", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("04:01 AM", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("04:01 PM", 3, false));

    // Test forbidden values
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(LocalTime.now(), 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(OffsetTime.now(), 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(new Date(), 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(1.5f, 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(1.5, 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("1.5", 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("1.0", 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(new Object(), 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(false, 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("", 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16("foo", 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16(java.sql.Time.valueOf("20:57:00"), 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampNtzSb16(java.sql.Date.valueOf("2010-11-03"), 3, false));
    expectError(
        ErrorCode.INVALID_ROW,
        () ->
            validateAndParseTimestampNtzSb16(
                java.sql.Timestamp.valueOf("2010-11-03 20:57:00"), 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(BigInteger.ZERO, 3, false));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16(BigDecimal.ZERO, 3, false));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampNtzSb16('c', 3, false));
  }

  @Test
  public void testValidateAndPareTimestampTz() {
    TimestampWrapper result =
        DataValidationUtil.validateAndParseTimestampTz("2021-01-01 01:00:00.123 +0100", 4);
    assertEquals(1609459200, result.getEpoch());
    assertEquals(1230, result.getFraction());
    assertEquals(Optional.of(3600000), result.getTimezoneOffset());
    assertEquals(Optional.of(1500), result.getTimeZoneIndex());

    // Time formats are not supported
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("20:57:01.123456789+07:00", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("20:57:01.123456789", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("20:57:01", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("20:57", 3));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("07:57:01.123456789 AM", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("04:01:07 AM", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("04:01 AM", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("04:01 PM", 3));

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(LocalTime.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(OffsetTime.now(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(new Date(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(1.5f, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(1.5, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("1.5", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("1.0", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(new Object(), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(false, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("", 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz("foo", 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampTz(java.sql.Time.valueOf("20:57:00"), 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampTz(java.sql.Date.valueOf("2010-11-03"), 3));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseTimestampTz(java.sql.Timestamp.valueOf("2010-11-03 20:57:00"), 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(BigInteger.ZERO, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz(BigDecimal.ZERO, 3));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseTimestampTz('c', 3));
  }

  @Test
  public void testValidateAndParseBigDecimal() {
    assertEquals(new BigDecimal("1"), validateAndParseBigDecimal("1"));
    assertEquals(
        new BigDecimal("1000").toBigInteger(), validateAndParseBigDecimal("1e3").toBigInteger());
    assertEquals(
        new BigDecimal("-1000").toBigInteger(), validateAndParseBigDecimal("-1e3").toBigInteger());
    assertEquals(
        new BigDecimal("1").toBigInteger(), validateAndParseBigDecimal("1e0").toBigInteger());
    assertEquals(
        new BigDecimal("-1").toBigInteger(), validateAndParseBigDecimal("-1e0").toBigInteger());
    assertEquals(
        new BigDecimal("123").toBigInteger(), validateAndParseBigDecimal("1.23e2").toBigInteger());
    assertEquals(
        new BigDecimal("123.4").toBigInteger(),
        validateAndParseBigDecimal("1.234e2").toBigInteger());
    assertEquals(
        new BigDecimal("0.1234").toBigInteger(),
        validateAndParseBigDecimal("1.234e-1").toBigInteger());
    assertEquals(
        new BigDecimal("0.1234").toBigInteger(),
        validateAndParseBigDecimal("1234e-5").toBigInteger());
    assertEquals(
        new BigDecimal("0.1234").toBigInteger(),
        validateAndParseBigDecimal("1234E-5").toBigInteger());
    assertEquals(new BigDecimal("1"), validateAndParseBigDecimal(1));
    assertEquals(new BigDecimal("1.0"), validateAndParseBigDecimal(1D));
    assertEquals(new BigDecimal("1"), validateAndParseBigDecimal(1L));
    assertEquals(new BigDecimal("1.0"), validateAndParseBigDecimal(1F));
    assertEquals(
        BigDecimal.valueOf(10).pow(37), validateAndParseBigDecimal(BigDecimal.valueOf(10).pow(37)));
    assertEquals(
        BigDecimal.valueOf(-1).multiply(BigDecimal.valueOf(10).pow(37)),
        validateAndParseBigDecimal(
            BigInteger.valueOf(-1).multiply(BigInteger.valueOf(10).pow(37))));

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, "honk");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, "0x22");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, true);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, false);
    expectError(
        ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, new Object());
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, 'a');
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, new byte[4]);
  }

  @Test
  public void testValidateAndParseString() {
    assertEquals("honk", validateAndParseString("honk", Optional.empty()));

    // Check max String length
    StringBuilder longBuilder = new StringBuilder();
    for (int i = 0; i < 16 * 1024 * 1024; i++) {
      longBuilder.append("č"); // max string length is measured in chars, not bytes
    }

    String maxString = longBuilder.toString();

    // max length + 1 should fail
    Assert.assertEquals(maxString, validateAndParseString(maxString, Optional.empty()));

    // Test validation
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseVariantAsString(longBuilder.append('a').toString()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseString("12345", Optional.of(4)));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseString(false, Optional.of(4)));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseString(12345, Optional.of(4)));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseString(1.2345, Optional.of(4)));

    // Test unsupported values
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseString(new Object(), Optional.empty()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseString(new byte[] {}, Optional.of(4)));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseString(new char[] {}, Optional.of(4)));
  }

  @Test
  public void testValidateAndParseVariant() throws Exception {
    String stringVariant = "{\"key\":1}";
    assertEquals(stringVariant, validateAndParseVariantAsString(stringVariant));
    JsonNode nodeVariant = objectMapper.readTree(stringVariant);
    assertEquals(stringVariant, validateAndParseVariantAsString(nodeVariant));

    // Test forbidden values
    expectError(
        ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseVariantAsString, new Object());
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseVariantAsString, "foo");
    expectError(
        ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseVariantAsString, new Date());
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseVariantAsString,
        Collections.singletonList(new Object()));
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseVariantAsString,
        Collections.singletonList(Collections.singletonMap("foo", new Object())));
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseVariantAsString,
        Collections.singletonMap(new Object(), "foo"));
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseVariantAsString,
        Collections.singletonMap("foo", new Object()));
  }

  @Test
  public void testValidateAndParseArray() throws Exception {
    int[] intArray = new int[] {1, 2, 3};
    assertEquals("[1,2,3]", validateAndParseArray(intArray));

    String[] stringArray = new String[] {"a", "b", "c"};
    assertEquals("[\"a\",\"b\",\"c\"]", validateAndParseArray(stringArray));

    Object[] objectArray = new Object[] {1, 2, 3};
    assertEquals("[1,2,3]", validateAndParseArray(objectArray));

    Object[] ObjectArrayWithNull = new Object[] {1, null, 3};
    assertEquals("[1,null,3]", validateAndParseArray(ObjectArrayWithNull));

    Object[][] nestedArray = new Object[][] {{1, 2, 3}, null, {4, 5, 6}};
    assertEquals("[[1,2,3],null,[4,5,6]]", validateAndParseArray(nestedArray));

    List<Integer> intList = Arrays.asList(1, 2, 3);
    assertEquals("[1,2,3]", validateAndParseArray(intList));

    List<Object> objectList = Arrays.asList(1, 2, 3);
    assertEquals("[1,2,3]", validateAndParseArray(objectList));

    List<Object> nestedList = Arrays.asList(Arrays.asList(1, 2, 3), 2, 3);
    assertEquals("[[1,2,3],2,3]", validateAndParseArray(nestedList));

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseArray, new Object());
    expectError(
        ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseArray, "foo"); // invalid JSON
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseArray, new Date());
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseArray,
        Collections.singletonList(new Object()));
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseArray,
        Collections.singletonList(Collections.singletonMap("foo", new Object())));
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseArray,
        Collections.singletonMap(new Object(), "foo"));
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseArray,
        Collections.singletonMap("foo", new Object()));
  }

  @Test
  public void testValidateAndParseObject() throws Exception {
    String stringObject = "{\"key\":1}";
    assertEquals(stringObject, validateAndParseObject(stringObject));
    JsonNode nodeObject = objectMapper.readTree(stringObject);
    assertEquals(stringObject, validateAndParseObject(nodeObject));

    String badObject = "foo";
    try {
      validateAndParseObject(badObject);
      Assert.fail("Expected INVALID_ROW error");
    } catch (SFException err) {
      assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), err.getVendorCode());
    }

    char[] data = new char[20000000];
    Arrays.fill(data, 'a');
    String stringVal = new String(data);
    Map<String, String> mapVal = new HashMap<>();
    mapVal.put("key", stringVal);
    String tooLargeObject = objectMapper.writeValueAsString(mapVal);
    try {
      validateAndParseObject(tooLargeObject);
      Assert.fail("Expected INVALID_ROW error");
    } catch (SFException err) {
      assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), err.getVendorCode());
    }

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseObject, "[]");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseObject, "1");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseObject, 1);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseObject, 1.5);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseObject, false);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseObject, new Object());
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseObject, "foo");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseObject, new Date());
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseObject,
        Collections.singletonList(new Object()));
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseObject,
        Collections.singletonList(Collections.singletonMap("foo", new Object())));
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseObject,
        Collections.singletonMap(new Object(), "foo"));
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseObject,
        Collections.singletonMap("foo", new Object()));
  }

  @Test
  public void testTooLargeVariant() {
    char[] stringContent = new char[16 * 1024 * 1024 - 16]; // {"a":"11","b":""}
    Arrays.fill(stringContent, 'c');

    // {"a":"11","b":""}
    Map<String, Object> m = new HashMap<>();
    m.put("a", "11");
    m.put("b", new String(stringContent));
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseVariantAsString, m);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseArray, m);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseObject, m);
  }

  @Test
  public void testTooLargeMultiByteSemiStructuredValues() {
    // Variant max size is not in characters, but in bytes
    char[] stringContent = new char[9 * 1024 * 1024]; // 8MB < value < 16MB
    Arrays.fill(stringContent, 'Č');

    Map<String, Object> m = new HashMap<>();
    m.put("a", new String(stringContent));
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseVariantAsString, m);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseArray, m);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseObject, m);
  }

  @Test
  public void testValidateAndParseDate() {
    assertEquals(-923, validateAndParseDate("1967-06-23"));
    assertEquals(-923, validateAndParseDate("1967-06-23 01:01:01"));
    assertEquals(18464, validateAndParseDate("2020-07-21"));
    assertEquals(18464, validateAndParseDate("2020-07-21 23:31:00"));

    // Time formats are not supported
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("20:57:01.123456789+07:00"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("20:57:01.123456789"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("20:57:01"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("20:57"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("07:57:01.123456789 AM"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("04:01:07 AM"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("04:01 AM"));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseDate("04:01 PM"));

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, new Object());
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, LocalTime.now());
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, OffsetTime.now());
    expectError(
        ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, new java.util.Date());
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, false);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, "");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, "foo");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, "1.0");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, 'c');
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, 1);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, 1L);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, 1.25);
    expectError(
        ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, BigInteger.valueOf(1));
    expectError(
        ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseDate, BigDecimal.valueOf(1.25));
  }

  @Test
  public void testValidateAndParseBinary() {
    byte[] maxAllowedArray = new byte[8 * 1024 * 1024];

    assertArrayEquals(
        "honk".getBytes(StandardCharsets.UTF_8),
        validateAndParseBinary("honk".getBytes(StandardCharsets.UTF_8), Optional.empty()));

    assertArrayEquals(
        DatatypeConverter.parseHexBinary("12"), validateAndParseBinary("12", Optional.empty()));

    assertArrayEquals(maxAllowedArray, validateAndParseBinary(maxAllowedArray, Optional.empty()));

    // Too large arrays should be rejected
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary(new byte[1], Optional.of(0)));
    expectError(
        ErrorCode.INVALID_ROW,
        () -> validateAndParseBinary(new byte[8 * 1024 * 1024 + 1], Optional.empty()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary(new byte[8], Optional.of(7)));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary("111", Optional.of(1)));

    // unsupported data types should fail
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary("111", Optional.empty()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary("foo", Optional.empty()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary("c", Optional.empty()));
    expectError(
        ErrorCode.INVALID_ROW,
        () ->
            validateAndParseBinary(Arrays.asList((byte) 1, (byte) 2, (byte) 3), Optional.empty()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary(1, Optional.empty()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary(12, Optional.empty()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary(1.5, Optional.empty()));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseBinary(BigInteger.ONE, Optional.empty()));
    expectError(ErrorCode.INVALID_ROW, () -> validateAndParseBinary(false, Optional.empty()));
    expectError(
        ErrorCode.INVALID_ROW, () -> validateAndParseBinary(new Object(), Optional.empty()));
  }

  @Test
  public void testValidateAndParseReal() throws Exception {
    // From number types
    assertEquals(1.23d, validateAndParseReal(1.23f), 0);
    assertEquals(1.23d, validateAndParseReal(1.23), 0);
    assertEquals(1.23d, validateAndParseReal(1.23d), 0);
    assertEquals(1.23d, validateAndParseReal(new BigDecimal("1.23")), 0);

    // From string
    assertEquals(1.23d, validateAndParseReal("1.23"), 0);
    assertEquals(123d, validateAndParseReal("1.23E2"), 0);
    assertEquals(123d, validateAndParseReal("1.23e2"), 0);

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseReal, "foo");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseReal, 'c');
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseReal, new Object());
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseReal, false);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseReal, true);
  }

  @Test
  public void testValidateAndParseBoolean() {

    for (Object input :
        Arrays.asList(
            true, "true", "True", "TruE", "t", "yes", "YeS", "y", "on", "1", 1.1, -1.1, -10, 10)) {
      assertEquals(1, validateAndParseBoolean(input));
    }

    for (Object input :
        Arrays.asList(false, "false", "False", "FalsE", "f", "no", "NO", "n", "off", "0", 0)) {
      assertEquals(0, validateAndParseBoolean(input));
    }

    // Test forbidden values
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBoolean, new Object());
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBoolean, 't');
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBoolean, 'f');
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBoolean, new int[] {});
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBoolean, "foobar");
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBoolean, "");
  }

  @Test
  public void testValidVariantType() {
    // Test primitive types
    Assert.assertTrue(isAllowedVariantType((byte) 1));
    Assert.assertTrue(isAllowedVariantType((short) 1));
    Assert.assertTrue(isAllowedVariantType(1));
    Assert.assertTrue(isAllowedVariantType(1L));
    Assert.assertTrue(isAllowedVariantType(1.25f));
    Assert.assertTrue(isAllowedVariantType(1.25d));
    Assert.assertTrue(isAllowedVariantType(false));
    Assert.assertTrue(isAllowedVariantType('c'));

    // Test boxed primitive types
    Assert.assertTrue(isAllowedVariantType(Byte.valueOf((byte) 1)));
    Assert.assertTrue(isAllowedVariantType(Short.valueOf((short) 1)));
    Assert.assertTrue(isAllowedVariantType(Integer.valueOf(1)));
    Assert.assertTrue(isAllowedVariantType(Long.valueOf(1L)));
    Assert.assertTrue(isAllowedVariantType(Float.valueOf(1.25f)));
    Assert.assertTrue(isAllowedVariantType(Double.valueOf(1.25d)));
    Assert.assertTrue(isAllowedVariantType(Boolean.valueOf(false)));
    Assert.assertTrue(isAllowedVariantType(Character.valueOf('c')));

    // Test primitive arrays
    Assert.assertTrue(isAllowedVariantType(new byte[] {1}));
    Assert.assertTrue(isAllowedVariantType(new short[] {1}));
    Assert.assertTrue(isAllowedVariantType(new int[] {1}));
    Assert.assertTrue(isAllowedVariantType(new long[] {1L}));
    Assert.assertTrue(isAllowedVariantType(new float[] {1.25f}));
    Assert.assertTrue(isAllowedVariantType(new double[] {1.25d}));
    Assert.assertTrue(isAllowedVariantType(new boolean[] {false}));
    Assert.assertTrue(isAllowedVariantType(new char[] {'c'}));

    // Test primitive lists
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList((byte) 1)));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList((short) 1)));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList(1)));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList(1L)));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList(1.25f)));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList(1.25d)));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList(false)));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList('c')));

    // Test additional numeric types and their collections
    Assert.assertTrue(isAllowedVariantType(new BigInteger("1")));
    Assert.assertTrue(isAllowedVariantType(new BigInteger[] {new BigInteger("1")}));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList(new BigInteger("1"))));
    Assert.assertTrue(isAllowedVariantType(new BigDecimal("1.25")));
    Assert.assertTrue(isAllowedVariantType(new BigDecimal[] {new BigDecimal("1.25")}));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList(new BigDecimal("1.25"))));

    // Test strings
    Assert.assertTrue(isAllowedVariantType("foo"));
    Assert.assertTrue(isAllowedVariantType(new String[] {"foo"}));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList("foo")));

    // Test date/time objects and their collections
    Assert.assertTrue(isAllowedVariantType(LocalTime.now()));
    Assert.assertTrue(isAllowedVariantType(OffsetTime.now()));
    Assert.assertTrue(isAllowedVariantType(LocalDate.now()));
    Assert.assertTrue(isAllowedVariantType(LocalDateTime.now()));
    Assert.assertTrue(isAllowedVariantType(ZonedDateTime.now()));
    Assert.assertTrue(isAllowedVariantType(OffsetDateTime.now()));
    Assert.assertTrue(isAllowedVariantType(new LocalTime[] {LocalTime.now()}));
    Assert.assertTrue(isAllowedVariantType(new OffsetTime[] {OffsetTime.now()}));
    Assert.assertTrue(isAllowedVariantType(new LocalDate[] {LocalDate.now()}));
    Assert.assertTrue(isAllowedVariantType(new LocalDateTime[] {LocalDateTime.now()}));
    Assert.assertTrue(isAllowedVariantType(new ZonedDateTime[] {ZonedDateTime.now()}));
    Assert.assertTrue(isAllowedVariantType(new OffsetDateTime[] {OffsetDateTime.now()}));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList(LocalTime.now())));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList(OffsetTime.now())));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList(LocalDate.now())));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList(LocalDateTime.now())));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList(ZonedDateTime.now())));
    Assert.assertTrue(isAllowedVariantType(Collections.singletonList(OffsetDateTime.now())));

    // Test mixed collections
    Assert.assertTrue(
        isAllowedVariantType(
            new Object[] {
              1,
              false,
              new BigInteger("1"),
              LocalDateTime.now(),
              new Object[] {new Object[] {new Object[] {LocalDateTime.now(), false}}}
            }));
    Assert.assertFalse(
        isAllowedVariantType(
            new Object[] {
              1,
              false,
              new BigInteger("1"),
              LocalDateTime.now(),
              new Object[] {new Object[] {new Object[] {new Object(), false}}}
            }));
    Assert.assertTrue(
        isAllowedVariantType(
            Arrays.asList(
                new BigInteger("1"),
                "foo",
                false,
                Arrays.asList(13, Arrays.asList(Arrays.asList(false, 'c'))))));
    Assert.assertFalse(
        isAllowedVariantType(
            Arrays.asList(
                new BigInteger("1"),
                "foo",
                false,
                Arrays.asList(13, Arrays.asList(Arrays.asList(new Object(), 'c'))))));

    // Test maps
    Assert.assertTrue(isAllowedVariantType(Collections.singletonMap("foo", "bar")));
    Assert.assertFalse(isAllowedVariantType(Collections.singletonMap(new Object(), "foo")));
    Assert.assertFalse(isAllowedVariantType(Collections.singletonMap("foo", new Object())));
    Assert.assertTrue(
        isAllowedVariantType(
            Collections.singletonMap(
                "foo",
                new Object[] {
                  1,
                  false,
                  new BigInteger("1"),
                  LocalDateTime.now(),
                  new Object[] {new Object[] {new Object[] {LocalDateTime.now(), false}}}
                })));
    Assert.assertFalse(
        isAllowedVariantType(
            Collections.singletonMap(
                "foo",
                new Object[] {
                  1,
                  false,
                  new BigInteger("1"),
                  LocalDateTime.now(),
                  new Object[] {new Object[] {new Object[] {new Object(), false}}}
                })));
    Assert.assertTrue(
        isAllowedVariantType(
            Collections.singletonMap(
                "foo",
                Arrays.asList(
                    new BigInteger("1"),
                    "foo",
                    false,
                    Arrays.asList(13, Arrays.asList(Arrays.asList(false, 'c')))))));
    Assert.assertFalse(
        isAllowedVariantType(
            Collections.singletonMap(
                "foo",
                Arrays.asList(
                    new BigInteger("1"),
                    "foo",
                    false,
                    Arrays.asList(13, Arrays.asList(Arrays.asList(new Object(), 'c')))))));
  }
}
