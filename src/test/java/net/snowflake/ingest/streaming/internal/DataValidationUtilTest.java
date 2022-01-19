package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.DataValidationUtil.MAX_BIGINTEGER;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.xml.bind.DatatypeConverter;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Test;

public class DataValidationUtilTest {
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final Object[] goodIntegersValue10 =
      new Object[] {10D, 10F, 10L, new BigInteger("10"), 10, "10", "1e1", "1.0e1"};

  private static final Object[] trueBooleanInput =
      new Object[] {true, "true", "True", "TruE", "t", "yes", "YeS", "y", "on", "1", 1.1};
  private static final Object[] falseBooleanInput =
      new Object[] {false, "false", "False", "FalsE", "f", "no", "NO", "n", "off", "0", 0};

  private void expectError(ErrorCode expectedError, Function func, Object args) {
    try {
      func.apply(args);
      Assert.fail("Expected Exception");
    } catch (SFException e) {
      Assert.assertEquals(expectedError.getMessageCode(), e.getVendorCode());
    } catch (Exception e) {
      Assert.fail("Invalid error through");
    }
  }

  @Test
  public void testValidateAndParseShort() {
    short e = 12;
    Assert.assertEquals(e, DataValidationUtil.validateAndParseShort("12"));
    Assert.assertEquals(e, DataValidationUtil.validateAndParseShort(e));
    Assert.assertEquals(Short.MAX_VALUE, DataValidationUtil.validateAndParseShort(Short.MAX_VALUE));
    Assert.assertEquals(Short.MIN_VALUE, DataValidationUtil.validateAndParseShort(Short.MIN_VALUE));

    // Expect errors
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseShort, "howdy");
    expectError(
        ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseShort, Short.MAX_VALUE + 1);
  }

  @Test
  public void testValidateAndParseTime() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(ArrowRowBuffer.COLUMN_SCALE, "0");
    Assert.assertEquals(
            new BigInteger("1595289600"),
            DataValidationUtil.validateAndParseTime("1595289600", metadata));

    metadata.put(ArrowRowBuffer.COLUMN_SCALE, "0");
    Assert.assertEquals(
            new BigInteger("1595289600"),
            DataValidationUtil.validateAndParseTime("2020-07-21", metadata));

    Assert.assertEquals(
            new BigInteger("1595374380"),
            DataValidationUtil.validateAndParseTime("2020-07-21 23:33:00", metadata));

    metadata.put(ArrowRowBuffer.COLUMN_SCALE, "3");
    Assert.assertEquals(
            new BigInteger("1595289600000"),
            DataValidationUtil.validateAndParseTime("1595289600", metadata));

    Assert.assertEquals(
            new BigInteger("1595289600000"),
            DataValidationUtil.validateAndParseTime("2020-07-21", metadata));

    Assert.assertEquals(
            new BigInteger("1595374380000"),
            DataValidationUtil.validateAndParseTime("2020-07-21 23:33:00", metadata));

    Assert.assertEquals(
            new BigInteger("1595374380123"),
            DataValidationUtil.validateAndParseTime("2020-07-21 23:33:00.123", metadata));
  }

  @Test
  public void testValidateAndPareTimestampNtzSb16() {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(ArrowRowBuffer.COLUMN_SCALE, "3");
    Assert.assertEquals(
        new TimestampWrapper(1, 123000000, BigInteger.valueOf(1123)),
        DataValidationUtil.validateAndParseTimestampNtzSb16("1.123", metadata));
    metadata.put(ArrowRowBuffer.COLUMN_SCALE, "9");
    Assert.assertEquals(
        new TimestampWrapper(1, 123, BigInteger.valueOf(1000000123)),
        DataValidationUtil.validateAndParseTimestampNtzSb16("1.000000123", metadata));

    // Expect errors
    try {
      DataValidationUtil.validateAndParseTimestampNtzSb16("honk", metadata);
      Assert.fail("Expected Exception");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }
    metadata.put(ArrowRowBuffer.COLUMN_SCALE, "1");
    try {
      DataValidationUtil.validateAndParseTimestampNtzSb16("1.23", metadata);
      Assert.fail("Expected Exception");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testValidateAndParseBigInteger() {
    for (Object input : goodIntegersValue10) {
      Assert.assertEquals(
          new BigInteger("10"), DataValidationUtil.validateAndParseBigInteger(input));
    }
    Assert.assertEquals(
        new BigInteger("-1000"), DataValidationUtil.validateAndParseBigInteger("-1e3"));

    Assert.assertEquals(
        BigInteger.valueOf(10).pow(37),
        DataValidationUtil.validateAndParseBigInteger(BigInteger.valueOf(10).pow(37)));
    Assert.assertEquals(
        BigInteger.valueOf(-1).multiply(BigInteger.valueOf(10).pow(37)),
        DataValidationUtil.validateAndParseBigInteger(
            BigInteger.valueOf(-1).multiply(BigInteger.valueOf(10).pow(37))));

    // Expect errors
    // Too big
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseBigInteger,
        BigInteger.valueOf(10).pow(38));
    // Too small
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseBigInteger,
        BigInteger.valueOf(-1).multiply(BigInteger.valueOf(10).pow(38)));
    // Decimal
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigInteger, 1.1D);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigInteger, 1.1F);
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigInteger, "1.1");
  }

  @Test
  public void testValidateAndParseBigDecimal() {
    Assert.assertEquals(new BigDecimal("1"), DataValidationUtil.validateAndParseBigDecimal("1"));
    Assert.assertEquals(
        new BigDecimal("1000").toBigInteger(),
        DataValidationUtil.validateAndParseBigDecimal("1e3").toBigInteger());
    Assert.assertEquals(
        new BigDecimal("-1000").toBigInteger(),
        DataValidationUtil.validateAndParseBigDecimal("-1e3").toBigInteger());
    Assert.assertEquals(
        new BigDecimal("1").toBigInteger(),
        DataValidationUtil.validateAndParseBigDecimal("1e0").toBigInteger());
    Assert.assertEquals(
        new BigDecimal("-1").toBigInteger(),
        DataValidationUtil.validateAndParseBigDecimal("-1e0").toBigInteger());
    Assert.assertEquals(
        new BigDecimal("123").toBigInteger(),
        DataValidationUtil.validateAndParseBigDecimal("1.23e2").toBigInteger());
    Assert.assertEquals(new BigDecimal("1"), DataValidationUtil.validateAndParseBigDecimal(1));
    Assert.assertEquals(new BigDecimal("1.0"), DataValidationUtil.validateAndParseBigDecimal(1D));
    Assert.assertEquals(new BigDecimal("1"), DataValidationUtil.validateAndParseBigDecimal(1L));
    Assert.assertEquals(new BigDecimal("1.0"), DataValidationUtil.validateAndParseBigDecimal(1F));
    Assert.assertEquals(
        BigDecimal.valueOf(10).pow(37),
        DataValidationUtil.validateAndParseBigDecimal(BigDecimal.valueOf(10).pow(37)));
    Assert.assertEquals(
        BigDecimal.valueOf(-1).multiply(BigDecimal.valueOf(10).pow(37)),
        DataValidationUtil.validateAndParseBigDecimal(
            BigInteger.valueOf(-1).multiply(BigInteger.valueOf(10).pow(37))));

    // Expect errors
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, "honk");
    expectError(
        ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBigDecimal, MAX_BIGINTEGER);
  }

  @Test
  public void testValidateAndParseString() {
    Assert.assertEquals(
        "honk", DataValidationUtil.validateAndParseString("honk", Optional.empty()));

    // Check max String length
    StringBuilder longBuilder = new StringBuilder();
    for (int i = 0; i < DataValidationUtil.MAX_STRING_LENGTH + 1; i++) {
      longBuilder.append("a");
    }
    String tooLong = longBuilder.toString();

    try {
      DataValidationUtil.validateAndParseString(tooLong, Optional.empty());
      Assert.fail("Expected error for String too long");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }

    try {
      DataValidationUtil.validateAndParseString("123", Optional.of(2));
      Assert.fail("Expected error for String too long");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    }
  }

  @Test
  public void testValidateAndParseVariant() throws Exception {
    String stringVariant = "{\"key\":1}";
    Assert.assertEquals(stringVariant, DataValidationUtil.validateAndParseVariant(stringVariant));
    JsonNode nodeVariant = objectMapper.readTree(stringVariant);
    Assert.assertEquals(stringVariant, DataValidationUtil.validateAndParseVariant(nodeVariant));
  }

  @Test
  public void testValidateAndParseInteger() {
    for (Object input : goodIntegersValue10) {
      Assert.assertEquals(10, DataValidationUtil.validateAndParseInteger(input));
    }

    // Bad inputs
    // Double
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseInteger, 10.1D);
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        Double.valueOf(Integer.MAX_VALUE) + 1);
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        Double.valueOf(Integer.MIN_VALUE) - 1);

    // Float
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseInteger, 10.1F);
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        Float.valueOf(Integer.MAX_VALUE) * 2);
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        Float.valueOf(Integer.MIN_VALUE) * 2);

    // Long
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        Long.valueOf(Integer.MAX_VALUE) + 1);
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        Long.valueOf(Integer.MIN_VALUE) - 1);

    // BigInteger
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        BigInteger.valueOf(Integer.MAX_VALUE).add(new BigInteger("1")));
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        BigInteger.valueOf(Integer.MIN_VALUE).add(new BigInteger("-1")));

    // String
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        "Honk goes the noble goose");
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        BigInteger.valueOf(Integer.MAX_VALUE).add(new BigInteger("1")).toString());
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseInteger,
        BigInteger.valueOf(Integer.MIN_VALUE).add(new BigInteger("-1")).toString());
  }

  @Test
  public void testValidateAndParseLong() {
    for (Object input : goodIntegersValue10) {
      Assert.assertEquals(10, DataValidationUtil.validateAndParseLong(input));
    }

    // Bad inputs
    // Double
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseLong, 10.1D);
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseLong,
        Double.valueOf(Long.MAX_VALUE) * 2);
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseLong,
        Double.valueOf(Long.MIN_VALUE) * 2);

    // Float
    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseLong, 10.1F);
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseLong,
        Float.valueOf(Long.MAX_VALUE) * 2);
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseLong,
        Float.valueOf(Long.MIN_VALUE) * 2);

    // BigInteger
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseLong,
        BigInteger.valueOf(Long.MAX_VALUE).add(new BigInteger("1")));
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseLong,
        BigInteger.valueOf(Long.MIN_VALUE).add(new BigInteger("-1")));

    // String
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseLong,
        "Honk goes the noble goose");
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseLong,
        BigInteger.valueOf(Long.MAX_VALUE).add(new BigInteger("1")).toString());
    expectError(
        ErrorCode.INVALID_ROW,
        DataValidationUtil::validateAndParseLong,
        BigInteger.valueOf(Long.MIN_VALUE).add(new BigInteger("-1")).toString());
  }

  @Test
  public void testValidateAndParseDate() {
    Assert.assertEquals(12341, DataValidationUtil.validateAndParseDate(12341));
  }

  @Test
  public void testGetStringValue() throws Exception {
    Assert.assertEquals("123", DataValidationUtil.getStringValue("123"));
    Assert.assertEquals("123", DataValidationUtil.getStringValue(123));
    Assert.assertEquals("123", DataValidationUtil.getStringValue(new BigDecimal("123")));
    Assert.assertEquals("123", DataValidationUtil.getStringValue(new BigInteger("123")));
    Assert.assertEquals("123.0", DataValidationUtil.getStringValue(123f));
    Assert.assertEquals("123.0", DataValidationUtil.getStringValue(123d));
    Assert.assertEquals("123", DataValidationUtil.getStringValue(123l));
  }

  @Test
  public void testGetTimestampInScale() throws Exception {
    Assert.assertEquals(
        new BigInteger("123"), DataValidationUtil.getTimeInScale("123.000000000", 0));
    Assert.assertEquals(new BigInteger("12301"), DataValidationUtil.getTimeInScale("123.01", 2));
  }

  @Test
  public void testValidateAndParseBinary() throws Exception {
    Assert.assertTrue(
        Arrays.equals(
            "honk".getBytes(StandardCharsets.UTF_8),
            DataValidationUtil.validateAndParseBinary("honk".getBytes(StandardCharsets.UTF_8))));
    Assert.assertTrue(
        Arrays.equals(
            DatatypeConverter.parseHexBinary("12"),
            DataValidationUtil.validateAndParseBinary("12")));

    Assert.assertTrue(
        Arrays.equals(
            DatatypeConverter.parseHexBinary("12"), DataValidationUtil.validateAndParseBinary(12)));

    expectError(ErrorCode.INVALID_ROW, DataValidationUtil::validateAndParseBinary, 123);
  }

  @Test
  public void testValidateAndParseReal() throws Exception {
    // From number types
    Assert.assertEquals(1.23d, DataValidationUtil.validateAndParseReal(1.23f), 0);
    Assert.assertEquals(1.23d, DataValidationUtil.validateAndParseReal(1.23), 0);
    Assert.assertEquals(1.23d, DataValidationUtil.validateAndParseReal(1.23d), 0);
    Assert.assertEquals(1.23d, DataValidationUtil.validateAndParseReal(new BigDecimal("1.23")), 0);

    // From string
    Assert.assertEquals(1.23d, DataValidationUtil.validateAndParseReal("1.23"), 0);
    Assert.assertEquals(123d, DataValidationUtil.validateAndParseReal("1.23E2"), 0);
    Assert.assertEquals(123d, DataValidationUtil.validateAndParseReal("1.23e2"), 0);

    // Error states
    try {
      DataValidationUtil.validateAndParseReal("honk");
      Assert.fail("Expected invalid row error");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), err.getVendorCode());
    }
  }

  @Test
  public void testValidateAndParseBoolean() throws Exception {
    for (Object input : trueBooleanInput) {
      Assert.assertEquals(1, DataValidationUtil.validateAndParseBoolean(input));
    }
    for (Object input : falseBooleanInput) {
      Assert.assertEquals(0, DataValidationUtil.validateAndParseBoolean(input));
    }

    // Error states
    try {
      DataValidationUtil.validateAndParseBoolean("honk");
      Assert.fail("Expected invalid row error");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), err.getVendorCode());
    }
  }

  @Test
  public void testConvertStringToBoolean() throws Exception {
    for (Object input : trueBooleanInput) {
      if (input instanceof String) {
        Assert.assertEquals(true, DataValidationUtil.convertStringToBoolean((String) input));
      }
    }

    for (Object input : falseBooleanInput) {
      if (input instanceof String) {
        Assert.assertEquals(false, DataValidationUtil.convertStringToBoolean((String) input));
      }
    }

    try {
      DataValidationUtil.convertStringToBoolean("honk");
      Assert.fail("Expected error");
    } catch (SFException e) {
      Assert.assertEquals(ErrorCode.INVALID_ROW.getMessageCode(), e.getVendorCode());
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
