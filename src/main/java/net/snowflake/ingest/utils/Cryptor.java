package net.snowflake.ingest.utils;

import static net.snowflake.ingest.utils.Constants.ENABLE_ENCRYPTION;
import static net.snowflake.ingest.utils.Constants.ENCRYPTION_ALGORITHM;

import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class Cryptor {

  /** Initialize 0 IV */
  private static final IvParameterSpec zeroIv = new IvParameterSpec(new byte[16]);

  /**
   * Hashes input bytes using SHA-256.
   *
   * @param input input bytes
   * @param offset offset into the output buffer to begin storing the digest
   * @param len number of bytes within buf allotted for the digest
   * @return SHA-256 hash
   */
  public static byte[] sha256Hash(byte[] input, int offset, int len) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(input, offset, len);
      return md.digest();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Hashes input bytes using SHA-256.
   *
   * @param input input bytes
   * @return SHA-256 hash
   */
  public static byte[] sha256Hash(byte[] input) {
    return sha256Hash(input, 0, input.length);
  }

  /**
   * Hashes input bytes using SHA-256 and converts hash into a string using Base64 encoding.
   *
   * @param input input bytes
   * @return Base64-encoded SHA-256 hash
   */
  public static String sha256HashBase64(byte[] input) {
    return Base64.getEncoder().encodeToString(sha256Hash(input));
  }

  /**
   * Diversifies a common master key using SHA-256, then constructs the SecretKey using the AES
   * algorithm.
   *
   * @param secretKey secret key
   * @param diversifier diversifier
   * @return diversified key
   */
  private static SecretKey deriveKey(String secretKey, String diversifier) {
    byte[] decodedString = Base64.getDecoder().decode(secretKey);
    byte[] concat = concat(decodedString, diversifier.getBytes(StandardCharsets.UTF_8));
    byte[] derivedBytes = sha256Hash(concat);
    return new SecretKeySpec(derivedBytes, "AES");
  }

  /**
   * Concatenates two byte arrays.
   *
   * @param a first byte array
   * @param b second byte array
   * @return resulting byte array
   */
  private static byte[] concat(byte[] a, byte[] b) {
    byte[] c = new byte[a.length + b.length];
    System.arraycopy(a, 0, c, 0, a.length);
    System.arraycopy(b, 0, c, a.length, b.length);
    return c;
  }

  /**
   * Encrypts input bytes using AES CTR mode with zero initialization vector.
   *
   * @param compressedChunkData bytes to encrypt
   * @param encryptionKey symmetric encryption key
   * @param diversifier diversifier for the encryption key
   * @return encrypted bytes, padded, prefixed with initialization vector
   */
  public static byte[] encrypt(byte[] compressedChunkData, String encryptionKey, String diversifier)
      throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException,
          InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
    if (!ENABLE_ENCRYPTION) {
      return compressedChunkData;
    }

    // Generate the derived key
    SecretKey derivedKey = deriveKey(encryptionKey, diversifier);

    // Encrypt with zero IV
    Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
    cipher.init(Cipher.ENCRYPT_MODE, derivedKey, zeroIv);
    return cipher.doFinal(compressedChunkData);
  }

  /**
   * Decrypts input bytes using AES CTR mode with zero initialization vector,this is used in testing
   * only
   *
   * @param input bytes to encrypt
   * @param encryptionKey symmetric encryption key
   * @return decrypted ciphertext
   */
  @VisibleForTesting
  public static byte[] decrypt(byte[] input, String encryptionKey, String diversifier)
      throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidAlgorithmParameterException,
          InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
    if (!ENABLE_ENCRYPTION) {
      return input;
    }

    // Generate the derived key
    SecretKey derivedKey = deriveKey(encryptionKey, diversifier);

    // Decrypt with zero IV
    Cipher cipher = Cipher.getInstance(ENCRYPTION_ALGORITHM);
    cipher.init(Cipher.DECRYPT_MODE, derivedKey, zeroIv);
    return cipher.doFinal(input);
  }
}
