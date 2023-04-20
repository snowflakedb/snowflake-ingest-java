package net.snowflake.ingest.streaming.internal;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.Assert;
import org.junit.Test;

public class ChannelDataTest {

  @Test
  public void testGetCombinedColumnStatsMapNulls() {
    Map<String, RowBufferStats> left = new HashMap<>();
    RowBufferStats leftStats1 = new RowBufferStats("COL1");
    left.put("one", leftStats1);
    leftStats1.addIntValue(new BigInteger("10"));

    try {
      ChannelData.getCombinedColumnStatsMap(left, null);
      Assert.fail("Expected error for null input");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getVendorCode());
    }

    try {
      ChannelData.getCombinedColumnStatsMap(null, left);
      Assert.fail("Expected error for null input");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getVendorCode());
    }

    try {
      ChannelData.getCombinedColumnStatsMap(null, null);
      Assert.fail("Expected error for null input");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getVendorCode());
    }
  }

  @Test
  public void testGetCombinedColumnStatsMapMissingColumn() {
    Map<String, RowBufferStats> left = new HashMap<>();
    RowBufferStats leftStats1 = new RowBufferStats("COL1");
    left.put("one", leftStats1);
    leftStats1.addIntValue(new BigInteger("10"));

    Map<String, RowBufferStats> right = new HashMap<>();
    RowBufferStats rightStats1 = new RowBufferStats("COL1");
    right.put("foo", rightStats1);
    rightStats1.addIntValue(new BigInteger("11"));

    // Check for same size, key mismatch
    try {
      ChannelData.getCombinedColumnStatsMap(left, right);
      Assert.fail("Expected error for mismatched input");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getVendorCode());
    }

    // Check different sizes
    right.remove("foo");
    right.put("one", rightStats1);
    right.put("two", rightStats1);

    try {
      ChannelData.getCombinedColumnStatsMap(left, right);
      Assert.fail("Expected error for mismatched input");
    } catch (SFException err) {
      Assert.assertEquals(ErrorCode.INTERNAL_ERROR.getMessageCode(), err.getVendorCode());
    }
  }

  @Test
  public void testGetCombinedColumnStatsMap() {
    Map<String, RowBufferStats> left = new HashMap<>();
    Map<String, RowBufferStats> right = new HashMap<>();

    RowBufferStats leftStats1 = new RowBufferStats("COL1");
    RowBufferStats rightStats1 = new RowBufferStats("COL1");
    RowBufferStats leftStats2 = new RowBufferStats("COL1");
    RowBufferStats rightStats2 = new RowBufferStats("COL1");

    left.put("one", leftStats1);
    left.put("two", leftStats2);
    right.put("one", rightStats1);
    right.put("two", rightStats2);

    leftStats1.addIntValue(new BigInteger("10"));
    leftStats1.addIntValue(new BigInteger("15"));
    rightStats1.addIntValue(new BigInteger("11"));
    rightStats1.addIntValue(new BigInteger("13"));
    rightStats1.addIntValue(new BigInteger("17"));

    leftStats2.addBinaryValue("10".getBytes(StandardCharsets.UTF_8));
    leftStats2.addBinaryValue("15".getBytes(StandardCharsets.UTF_8));
    rightStats2.addBinaryValue("11".getBytes(StandardCharsets.UTF_8));
    rightStats2.addBinaryValue("13".getBytes(StandardCharsets.UTF_8));
    rightStats2.addBinaryValue("17".getBytes(StandardCharsets.UTF_8));

    Map<String, RowBufferStats> result = ChannelData.getCombinedColumnStatsMap(left, right);

    RowBufferStats oneCombined = result.get("one");
    RowBufferStats twoCombined = result.get("two");

    Assert.assertEquals(new BigInteger("10"), oneCombined.getCurrentMinIntValue());
    Assert.assertEquals(new BigInteger("17"), oneCombined.getCurrentMaxIntValue());
    Assert.assertEquals(-1, oneCombined.getDistinctValues());
    Assert.assertNull(oneCombined.getCurrentMinStrValue());
    Assert.assertNull(oneCombined.getCurrentMaxStrValue());
    Assert.assertNull(oneCombined.getCurrentMinRealValue());
    Assert.assertNull(oneCombined.getCurrentMaxRealValue());

    Assert.assertArrayEquals(
        "10".getBytes(StandardCharsets.UTF_8), twoCombined.getCurrentMinStrValue());
    Assert.assertArrayEquals(
        "17".getBytes(StandardCharsets.UTF_8), twoCombined.getCurrentMaxStrValue());
    Assert.assertEquals(-1, twoCombined.getDistinctValues());
    Assert.assertNull(twoCombined.getCurrentMinIntValue());
    Assert.assertNull(twoCombined.getCurrentMaxIntValue());
    Assert.assertNull(twoCombined.getCurrentMinRealValue());
    Assert.assertNull(twoCombined.getCurrentMaxRealValue());
  }
}
