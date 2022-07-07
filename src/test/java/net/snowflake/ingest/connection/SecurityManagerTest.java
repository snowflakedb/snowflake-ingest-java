package net.snowflake.ingest.connection;

import static org.junit.Assert.assertTrue;

import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Base64;
import org.junit.Assert;
import org.junit.Test;

public class SecurityManagerTest {
  private String expectedPublicKeyFp = "SHA256:yVUGJrOo4BN1Cza+m2zNzvQbk/4rICTydzSNvuiyy9Q=";

  /** BASE64 ENCODED PUBLIC KEY */
  private String storedPublicKey =
      "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtRPru42llC40VdmWnc8r7TI/AFemZw4Lh1HRnPIFRxwhOE/yxHHxFGuPLUouyHWM9rVT9N9eo6PTOB8TCnGwfwTW2jloSbjtycDdM3+UrBUpX7x/Ufhcwoeu0O3NR5pAhGJRVKCvSpmrD3k2l2vZsRL0230IPGxeDB8m2Wia8QCKKou7AkSsmQ3/9kcKowLGf2axPHty2QSXx4NKvwe0B1NnLcQTBc6Z83Lym3gKn8YSINk1ZoO9G5oQKr64wnuQIOlXjcXD8BAEYKbv7VkG1vsikqixFpPfWrGUlzhoWWTcn4awzRFaX81ZzhAtfA/laqVvrqN/+O6Cc1k614kVGQIDAQAB"; // pragma: allowlist secret

  /** BASE64 ENCODED PRIVATE KEY */
  private String storedPrivateKey =
      "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC1E+u7jaWULjRV2ZadzyvtMj8AV6ZnDguHUdGc8gVHHCE4T/LEcfEUa48tSi7IdYz2tVP0316jo9M4HxMKcbB/BNbaOWhJuO3JwN0zf5SsFSlfvH9R+FzCh67Q7c1HmkCEYlFUoK9KmasPeTaXa9mxEvTbfQg8bF4MHybZaJrxAIoqi7sCRKyZDf/2RwqjAsZ/ZrE8e3LZBJfHg0q/B7QHU2ctxBMFzpnzcvKbeAqfxhIg2TVmg70bmhAqvrjCe5Ag6VeNxcPwEARgpu/tWQbW+yKSqLEWk99asZSXOGhZZNyfhrDNEVpfzVnOEC18D+VqpW+uo3/47oJzWTrXiRUZAgMBAAECggEAd/XnkMwJKr7law6IaqmqJyzHchmfIty6JH6+yCPJ/U8UbvMAGMaHeQi8xLtFfQXrSjHcmfg1AWHx91cWzS9+RtfU4qNvhI+f8K31nT1jKBGo5ETDcHGCOlmbJcy01z/IiCt+R/tfxaNCITEjSnNnt5igYJxXjXgZYhxtJ1DWfNvJ0PEPRF+Wuo1y77EW4ACzAKdZriLuJ+ynakYIZc8TC+w2ZiSDHx0ZEQ2YmV7m7A11db1bAw4X7Z7ECvx9VaZKmvj3v9xa8BIcJtYw8YJSHL0f3g/xz+qAqDVw5X7zofdOFEwx96/o14lWnohVjruaGEQvMsNKNz/ONQ1cUaTsFQKBgQDlik5hjo7AEx8mlu5qvXt9iAqSSSI56bunJofbfRLihI9Z3e10v5pvdj9CwB/gJYtOJ4rYLo4KGY7AMq7rq4ObVGF3lIIdBQldy0B+w2jHaemNlg5shQKqC4dA26MlOKO0iIvS1HIazkRGk1sgp8NVaG7jMvGyzD75UsdPRFoBxwKBgQDJ838J+R2OjAdbcDKblfpc/O+IU5chxNswojlA4NLbROr9RBNj04r565g1kU0vbj14Cj9Ocifb9yZNQJPIPfyVVg8LIrt9YciCGnvUSdX/558+O7Y/HwqXlzGt3TRnHpdH34qO/CSUEl5kDP/TmW3sw6VNYZa1QwhWVldKgWYyHwKBgDzUomEIPpx4dNDtPtHa1Vc3LlYGO6PNZYWumGJ6iv6s0rCmN7+w52SSmcE+2TO1v20+3XTdIZdbnpEg3WpnUcFgY1QlbzXxl8Hbk4QElUgDsXlsQvZPaZ1W4Mk3a8z5bajyZtvAoVypPT7W3leRHhsMSha78YHIzweUAG3pV1ERAoGATEg4nVjG7FhCUyyvQQvGtScph3IjrTLBpL4yKCqEGyUOKjpzpIp8fWibZuiKojbe6x/bx9Lg8XqKsjWJXOLlLLeEGS22ambsKRC943M8bVxdT1GYxoEALECFGGps5+KrPA/ZM6dUXcYOd3Zdj9zto7hHEVKibbdzR8F3WYJFSvsCgYBj1TqNONCQRJvo298bJtm9RXzYQvjHXnNNnbEL+B+B/r2jBOM/t8WBLJVNghQDhJY+DMMdptxkvNge3XNenDtY25UsnB7XefRE0tDe6yLWKbONT33+WGZjCBCXpQ90Avvi9npFetwG9Q8GSP7VdbTctLwfZra8DDXs5Dz9Gion+A=="; // pragma: allowlist secret

  private String expectedPublicKeyFp2 = "SHA256:MDcEdlQsgzIs7UBLHV6CB9GJLqqW/AqGsMcAPrWVxuA=";

  private String storedPublicKey2 =
      "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwwtuB1ZFqe9jF8TjvwoHoGO2KRxSu8GRDuI93/g1dJKve/SsmNB+SPQ402tEmnejg6QMlyFOvh+bbEqYooXN6lCaFSk7DAx7aNqr1yU/Jpwzbal0H74PhOAw4u1iyBCf08r8aQHLYkOUF2DcggUI" // pragma: allowlist secret
          + " WCKrBnpEC6vK8aZRWGwpgXB46CkousWXrmKBqbEBJusj2/Fgrk2CZ/OGY/vlzh6A7TpucviZUF3bsmsEs//63XpTSQsL785uixJbnQye8HDN4iyjvK09dHruIfSVPZ2N7xPdw7Nvyf+gRBmu2HWCPFpOc7a0XPNarlQPXPLbGz47dIZNEW+8p2jdw1D2PZ3h" // pragma: allowlist secret
          + " vwIDAQAB"; // pragma: allowlist secret

  private String storedPrivateKey2 =
      "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDDC24HVkWp72MXxOO/CgegY7YpHFK7wZEO4j3f+DV0kq979KyY0H5I9DjTa0Sad6ODpAyXIU6+H5ts" // pragma: allowlist secret
          + " Spiihc3qUJoVKTsMDHto2qvXJT8mnDNtqXQfvg+E4DDi7WLIEJ/TyvxpActiQ5QXYNyCBQhYIqsGekQLq8rxplFYbCmBcHjoKSi6xZeuYoGpsQEm6yPb8WCuTYJn84Zj++XOHoDtOm5y+JlQXduyawSz//rdelNJCwvvzm6LEludDJ7wcM3iLKO8rT10eu4h9JU9nY3vE93Ds2/J/6BEGa7YdYI8Wk5ztrRc81quVA9c8tsbPjt0hk0Rb7ynaN3D" // pragma: allowlist secret
          + " UPY9neG/AgMBAAECggEBAL9JLGXBtJzPDC45iOrJWWVxpSt4faNqWWtxcyF++l4Tpks5UTSl9dRywHCImUWs5A6tCzQFFIbd1L5GAqAR/js5RYRPZXuRmk7hdvqPqvmg48c/E4Y2Dl5QyWElU2XG+Bjs0NPjUKZUhJ7Q/jH94YsepQC7VJTlrSmF5e2EVsh73wwAd+fRNSvWmMoVsjw1gX8Cen8rCEZZyPSwaK1pYBQga0abEZ6BdQQ0O3yTZQga" // pragma: allowlist secret
          + " Q3z+vC+hm3cqpAmhWupYxqdA/EBNz/v0GVe8BGbEYAnm/4LPpQilt1Z1ngNqdUoTzF4PXnYQ/fFOR90nbjWoJLJYCDd+QW4XXwxwOIs/BsECgYEA+USiF2F+lG7y6dhkfLAi8UROAjYv+/wWSBnOV0JCD0A6Ik5bOXvXs72/NCHdBGq93mbYhzo7ar9Fm0QEmtwfnfhVNRk+SSq/+hzCOmZrdZpu2nMzBDJtR2TartOwfijE/NNZGA1V7QOmR02uWINyaqvDNNPVIt+D6qreXcZDslECgYEAyE/pWfLjJfWBXtOruXxBcJKq5Arx0YSvi7VaiOg9zBtLg4imKvRJqjs6Y7rGap3qA+HguEl4TQHvhIfhlAxWJ2XVB0ijTAiFaMTmBmxCJq0V9raScJPbSHAN54T2f0x+yh5/q7IFFKF2Hn3qrmarSAw02G+KTH6MlFFqJ+Dcvw8CgYEA7aMX4LBqq3nGjVdmHVUSSu7ya7tbLaDbYStxAtFBBycVBQWshHXjYxD/SuUJvx9AGdn0jZ7fbFojMu26ciRu4/wOx4tkTP67fOeT53ci9UAgdJQky9iDQ/ALZ2abOPsHKX0X0A1OoKG9EPcmwm22U6midSeKZy+tpLf3PHE6srECgYAayj0+T3K7t+r2gL69zvV9ldAPMbuHtwQ3XijemJjzPE9MJzF6GzPi9Yronak9xyLuI/6HByR0wCaFhhrQTxoSqNbl43wbhiQ5j+PnxgDO5WVDmsVZEx1HwdzKMwk4m0V1yMBweR2e1b1TdKm3a3nK5/8FV12av24TxBO7g6JiVwKBgDnaedWvgJt+tJrkFn9hZE2VVGC4FjFDpCMxIebvBz5Kbs+lknpdcda8+DMwXFDf8OR3lt/3KzJqleUhrTTzAQz56Xdi9VEnVs3rsgvX9VnaWcRpa4GT5EIj+I2M9t+D8XCfMMs1S56Pnn5oGkqvFBzmMRnskqK6d75B8EG5BGi0"; // pragma: allowlist secret

  @Test
  public void validatePublicKeyFp() throws NoSuchAlgorithmException, InvalidKeySpecException {

    PublicKey pubKey = loadPublicKey(storedPublicKey);
    PrivateKey priKey = loadPrivateKey(storedPrivateKey);

    KeyPair keypair = new KeyPair(pubKey, priKey);

    String accountName = "accountName";
    String userName = "userName";
    SecurityManager securityManager = new SecurityManager(accountName, userName, keypair, null);
    String publicKeyFp = securityManager.getPublicKeyFingerPrint();
    assertTrue(publicKeyFp.equals(expectedPublicKeyFp));

    PublicKey pubKey2 = loadPublicKey(storedPublicKey2);
    PrivateKey priKey2 = loadPrivateKey(storedPrivateKey2);

    KeyPair keypair2 = new KeyPair(pubKey2, priKey2);

    SecurityManager securityManager2 = new SecurityManager(accountName, userName, keypair2, null);
    String publicKeyFp2 = securityManager2.getPublicKeyFingerPrint();
    assertTrue(publicKeyFp2.equals(expectedPublicKeyFp2));
  }

  @Test
  public void testParseAccount() throws NoSuchAlgorithmException, InvalidKeySpecException {
    PublicKey pubKey = loadPublicKey(storedPublicKey);
    PrivateKey priKey = loadPrivateKey(storedPrivateKey);

    KeyPair keypair = new KeyPair(pubKey, priKey);

    String accountName = "accountName";
    String userName = "userName";
    SecurityManager securityManager = new SecurityManager(accountName, userName, keypair, null);
    Assert.assertEquals(accountName.toUpperCase(), securityManager.getAccount());
  }

  @Test
  public void testParseAccount_dotInAccountName()
      throws NoSuchAlgorithmException, InvalidKeySpecException {
    PublicKey pubKey = loadPublicKey(storedPublicKey);
    PrivateKey priKey = loadPrivateKey(storedPrivateKey);

    KeyPair keypair = new KeyPair(pubKey, priKey);

    String accountName = "accountName.extra";
    String userName = "userName";
    String trimmedAccountName = "accountName";
    SecurityManager securityManager = new SecurityManager(accountName, userName, keypair, null);
    Assert.assertEquals(trimmedAccountName.toUpperCase(), securityManager.getAccount());
  }

  /**
   * Converts encodedBase64 publicKey back to the RSA scheme PublicKey object.
   *
   * <p>
   *
   * @param base64PublicKey
   * @return
   * @throws NoSuchAlgorithmException
   * @throws InvalidKeySpecException
   */
  private PublicKey loadPublicKey(String base64PublicKey)
      throws NoSuchAlgorithmException, InvalidKeySpecException {
    byte[] data = Base64.getMimeDecoder().decode(base64PublicKey);
    X509EncodedKeySpec spec = new X509EncodedKeySpec(data);
    KeyFactory factory = KeyFactory.getInstance("RSA");
    return factory.generatePublic(spec);
  }

  /**
   * Converts privateKey in encodedBase64 in String to RSA scheme PrivateKey.
   *
   * @param base64PrivateKey
   * @return
   * @throws NoSuchAlgorithmException
   * @throws InvalidKeySpecException
   */
  private PrivateKey loadPrivateKey(String base64PrivateKey)
      throws NoSuchAlgorithmException, InvalidKeySpecException {
    byte[] clear = Base64.getMimeDecoder().decode(base64PrivateKey);
    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(clear);
    KeyFactory factory = KeyFactory.getInstance("RSA");
    PrivateKey privateKey = factory.generatePrivate(keySpec);
    Arrays.fill(clear, (byte) 0);
    return privateKey;
  }
}
