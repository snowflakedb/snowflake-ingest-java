package net.snowflake.ingest.streaming.internal;

import static net.snowflake.ingest.streaming.internal.BinaryStringUtils.truncateBytesAsHex;
import static net.snowflake.ingest.streaming.internal.BinaryStringUtils.unicodeCharactersCount;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Test;

public class BinaryStringUtilsTest {

  @Test
  public void testUnicodeCodePointsCalculation() {
    Assert.assertEquals(1, unicodeCharactersCount("🍞"));
  }

  @Test
  public void testTruncation() throws DecoderException {
    // Test empty input
    Assert.assertEquals("", truncateBytesAsHex(new byte[0], false));
    Assert.assertEquals("", truncateBytesAsHex(new byte[0], true));

    // Test basic case
    Assert.assertEquals("aa", truncateBytesAsHex(Hex.decodeHex("aa"), false));
    Assert.assertEquals("aa", truncateBytesAsHex(Hex.decodeHex("aa"), true));

    // Test exactly 32 bytes
    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        truncateBytesAsHex(
            Hex.decodeHex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            false));
    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        truncateBytesAsHex(
            Hex.decodeHex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            true));

    Assert.assertEquals(
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        truncateBytesAsHex(
            Hex.decodeHex("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            false));

    Assert.assertEquals(
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        truncateBytesAsHex(
            Hex.decodeHex("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            true));

    // Test 1 truncate up
    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        truncateBytesAsHex(
            Hex.decodeHex(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            false));

    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
        truncateBytesAsHex(
            Hex.decodeHex(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            true));

    // Test one overflow
    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffaaff",
        truncateBytesAsHex(
            Hex.decodeHex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffaaffffffff"),
            false));
    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffab00",
        truncateBytesAsHex(
            Hex.decodeHex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffaaffffffff"),
            true));

    // Test many overflow
    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffffff",
        truncateBytesAsHex(
            Hex.decodeHex(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            false));
    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaab00000000000000000000000000000000000",
        truncateBytesAsHex(
            Hex.decodeHex(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffffffffffffffffffffffff"),
            true));

    // Test infinity
    Assert.assertEquals(
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        truncateBytesAsHex(
            Hex.decodeHex(
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccccccc"),
            false));
    Assert.assertEquals(
        "Z",
        truncateBytesAsHex(
            Hex.decodeHex(
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccccccc"),
            true));
  }
}
