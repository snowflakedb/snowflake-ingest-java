package net.snowflake.ingest.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class Cryptor
{
  /**
   * Hashes input bytes using SHA-256.
   *
   * @param input  input bytes
   * @param offset offset into the output buffer to begin storing the digest
   * @param len    number of bytes within buf allotted for the digest
   * @return SHA-256 hash
   */
  public static byte[] sha256Hash(byte[] input, int offset, int len)
  {
    try
    {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(input, offset, len);
      return md.digest();
    }
    catch (NoSuchAlgorithmException e)
    {
      throw new RuntimeException(e);
    }
  }

  /**
   * Hashes input bytes using SHA-256.
   *
   * @param input input bytes
   * @return SHA-256 hash
   */
  public static byte[] sha256Hash(byte[] input)
  {
    return sha256Hash(input, 0, input.length);
  }

  /**
   * Hashes input bytes using SHA-256 and converts hash into a string using
   * Base64 encoding.
   *
   * @param input input bytes
   * @return Base64-encoded SHA-256 hash
   */
  public static String sha256HashBase64(byte[] input)
  {
    return Base64.getEncoder().encodeToString(sha256Hash(input));
  }

}
