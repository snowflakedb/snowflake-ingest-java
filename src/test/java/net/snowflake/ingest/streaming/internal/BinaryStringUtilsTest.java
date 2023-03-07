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
    Assert.assertEquals("aa", truncateBytesAsHex(Hex.decodeHex("aa".toCharArray()), false));
    Assert.assertEquals("aa", truncateBytesAsHex(Hex.decodeHex("aa".toCharArray()), true));

    // Test exactly 32 bytes
    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        truncateBytesAsHex(
            Hex.decodeHex(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".toCharArray()),
            false));
    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        truncateBytesAsHex(
            Hex.decodeHex(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".toCharArray()),
            true));

    Assert.assertEquals(
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        truncateBytesAsHex(
            Hex.decodeHex(
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".toCharArray()),
            false));

    Assert.assertEquals(
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        truncateBytesAsHex(
            Hex.decodeHex(
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".toCharArray()),
            true));

    // Test 1 truncate up
    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        truncateBytesAsHex(
            Hex.decodeHex(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .toCharArray()),
            false));

    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
        truncateBytesAsHex(
            Hex.decodeHex(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .toCharArray()),
            true));

    // Test one overflow
    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffaaff",
        truncateBytesAsHex(
            Hex.decodeHex(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffaaffffffff"
                    .toCharArray()),
            false));
    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffab00",
        truncateBytesAsHex(
            Hex.decodeHex(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffaaffffffff"
                    .toCharArray()),
            true));

    // Test many overflow
    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffffff",
        truncateBytesAsHex(
            Hex.decodeHex(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffffffffffffffffffffffff"
                    .toCharArray()),
            false));
    Assert.assertEquals(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaab00000000000000000000000000000000000",
        truncateBytesAsHex(
            Hex.decodeHex(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaafffffffffffffffffffffffffffffffffffffffffffffffffffff"
                    .toCharArray()),
            true));

    // Test infinity
    Assert.assertEquals(
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
        truncateBytesAsHex(
            Hex.decodeHex(
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccccccc"
                    .toCharArray()),
            false));
    Assert.assertEquals(
        "Z",
        truncateBytesAsHex(
            Hex.decodeHex(
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffcccccccccccc"
                    .toCharArray()),
            true));
  }
}
