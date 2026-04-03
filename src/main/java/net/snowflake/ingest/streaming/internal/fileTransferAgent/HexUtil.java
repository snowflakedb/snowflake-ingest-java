/*
 * Replicated from snowflake-jdbc (v3.25.1)
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/HexUtil.java
 *
 * Permitted differences: package declaration.
 */
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

class HexUtil {

  /**
   * Converts Byte array to hex string
   *
   * @param bytes a byte array
   * @return a string in hexadecimal code
   */
  static String byteToHexString(byte[] bytes) {
    final char[] hexArray = "0123456789ABCDEF".toCharArray();
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = hexArray[v >>> 4];
      hexChars[j * 2 + 1] = hexArray[v & 0x0F];
    }
    return new String(hexChars);
  }
}
