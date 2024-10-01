package net.snowflake.ingest.streaming.internal.datatypes;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

public class IcebergDataTypeTest extends AbstractDataTypeTest {
    @Before
    public void before() throws Exception {
        super.before(true);
    }

    @Test
    public void testBoolean() throws Exception {
        // testIcebergIngestion("boolean", null, null);

        testIcebergIngestion("boolean", true, new BooleanProvider());
        testIcebergIngestion("boolean", false, new BooleanProvider());
        testIcebergIngestion("boolean", 1, true, new BooleanProvider());
        testIcebergIngestion("boolean", "false", false, new BooleanProvider());

        SFException ex = Assert.assertThrows(SFException.class, () -> testIcebergIngestion("boolean", new Object(), true, new BooleanProvider()));
        Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
    }

    @Test
    public void testInt() throws Exception {
        // testIcebergIngestion("int", null, null);

        testIcebergIngestion("int", 1, new IntProvider());
        testIcebergIngestion("int", -.0f, 0, new IntProvider());
        testIcebergIngestion("int", 0.5f, 1, new IntProvider());
        testIcebergIngestion("int", "100.4", 100, new IntProvider());
        testIcebergIngestion("int", new BigDecimal("1000000.09"), 1000000, new IntProvider());
        testIcebergIngestion("int", Integer.MAX_VALUE, new IntProvider());
        testIcebergIngestion("int", Integer.MIN_VALUE, new IntProvider());

        SFException ex = Assert.assertThrows(SFException.class, () -> testIcebergIngestion("int", Long.MAX_VALUE, new LongProvider()));
        Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), ex.getVendorCode());

        ex = Assert.assertThrows(SFException.class, () -> testIcebergIngestion("int", true, 0, new IntProvider()));
        Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
    }

    @Test
    public void testLong() throws Exception {
        // testIcebergIngestion("long", null, null);

        testIcebergIngestion("long", 1L, new LongProvider());
        testIcebergIngestion("long", -.0f, 0L, new LongProvider());
        testIcebergIngestion("long", 0.5f, 1L, new LongProvider());
        testIcebergIngestion("long", "100.4", 100L, new LongProvider());
        testIcebergIngestion("long", new BigDecimal("1000000.09"), 1000000L, new LongProvider());
        testIcebergIngestion("long", Long.MAX_VALUE, new LongProvider());
        testIcebergIngestion("long", Long.MIN_VALUE, new LongProvider());

        SFException ex = Assert.assertThrows(SFException.class, () -> testIcebergIngestion("long", Double.MAX_VALUE, new DoubleProvider()));
        Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), ex.getVendorCode());

        ex = Assert.assertThrows(SFException.class, () -> testIcebergIngestion("long", Double.NaN, new DoubleProvider()));
        Assert.assertEquals(ErrorCode.INVALID_VALUE_ROW.getMessageCode(), ex.getVendorCode());

        ex = Assert.assertThrows(SFException.class, () -> testIcebergIngestion("long", false, 0L, new LongProvider()));
        Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
    }

    @Test
    public void testFloat() throws Exception {
        // testIcebergIngestion("float", null, null);

        testIcebergIngestion("float", 1.0f, new FloatProvider());
        testIcebergIngestion("float", -.0f, .0f, new FloatProvider());
        testIcebergIngestion("float", Float.POSITIVE_INFINITY, new FloatProvider());
        testIcebergIngestion("float", "NaN", Float.NaN, new FloatProvider());
        testIcebergIngestion("float", new BigDecimal("1000.0"), 1000f, new FloatProvider());
        testIcebergIngestion("float", Double.MAX_VALUE, Float.POSITIVE_INFINITY, new FloatProvider());

        SFException ex = Assert.assertThrows(SFException.class, () -> testIcebergIngestion("float", new Object(), 1f, new FloatProvider()));
        Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
    }

    @Test
    public void testDouble() throws Exception {
        // testIcebergIngestion("double", null, null);

        testIcebergIngestion("double", 1.0, new DoubleProvider());
        testIcebergIngestion("double", -.0, .0, new DoubleProvider());
        testIcebergIngestion("double", Double.POSITIVE_INFINITY, new DoubleProvider());
        testIcebergIngestion("double", "NaN", Double.NaN, new DoubleProvider());
        testIcebergIngestion("double", new BigDecimal("1000.0"), 1000.0, new DoubleProvider());
        testIcebergIngestion("double", Double.MAX_VALUE, Double.MAX_VALUE, new DoubleProvider());

        SFException ex = Assert.assertThrows(SFException.class, () -> testIcebergIngestion("double", new Object(), 1.0, new DoubleProvider()));
        Assert.assertEquals(ErrorCode.INVALID_FORMAT_ROW.getMessageCode(), ex.getVendorCode());
    }

    @Test
    public void testDecimal() throws Exception {
        // testIcebergIngestion("decimal", null, null);

        testIcebergIngestion("decimal(3, 1)", new BigDecimal("-12.3"), new BigDecimalProvider());
        testIcebergIngestion("decimal(1, 0)", new BigDecimal("-0.0"), new BigDecimalProvider());
        testIcebergIngestion("decimal(3, 1)", 12.5f, new FloatProvider());
        testIcebergIngestion("decimal(3, 1)", -99, new IntProvider());
        testIcebergIngestion("decimal(38, 0)", Long.MAX_VALUE, new LongProvider());
        testIcebergIngestion("decimal(38, 10)", "1234567890123456789012345678.1234567890", new BigDecimal("1234567890123456789012345678.1234567890"), new BigDecimalProvider());
    }
}
