package net.snowflake.ingest.connection;


import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
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
import static org.junit.Assert.assertTrue;

public class SecurityManagerTest
{
  private String expectedPublicKeyFp =
      "SHA256:yVUGJrOo4BN1Cza+m2zNzvQbk/4rICTydzSNvuiyy9Q=";

  /**
   * BASE64 ENCODED PUBLIC KEY
   */
  private String storedPublicKey =
      "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtRPru42llC40VdmWnc8r\n" +
      "7TI/AFemZw4Lh1HRnPIFRxwhOE/yxHHxFGuPLUouyHWM9rVT9N9eo6PTOB8TCnGw\n" +
      "fwTW2jloSbjtycDdM3+UrBUpX7x/Ufhcwoeu0O3NR5pAhGJRVKCvSpmrD3k2l2vZ\n" +
      "sRL0230IPGxeDB8m2Wia8QCKKou7AkSsmQ3/9kcKowLGf2axPHty2QSXx4NKvwe0\n" +
      "B1NnLcQTBc6Z83Lym3gKn8YSINk1ZoO9G5oQKr64wnuQIOlXjcXD8BAEYKbv7VkG\n" +
      "1vsikqixFpPfWrGUlzhoWWTcn4awzRFaX81ZzhAtfA/laqVvrqN/+O6Cc1k614kV\n" +
      "GQIDAQAB\n";

  /**
   * BASE64 ENCODED PRIVATE KEY
   */
  private String storedPrivateKey =
      "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC1E+u7jaWULjRV\n" +
      "2ZadzyvtMj8AV6ZnDguHUdGc8gVHHCE4T/LEcfEUa48tSi7IdYz2tVP0316jo9M4\n" +
      "HxMKcbB/BNbaOWhJuO3JwN0zf5SsFSlfvH9R+FzCh67Q7c1HmkCEYlFUoK9KmasP\n" +
      "eTaXa9mxEvTbfQg8bF4MHybZaJrxAIoqi7sCRKyZDf/2RwqjAsZ/ZrE8e3LZBJfH\n" +
      "g0q/B7QHU2ctxBMFzpnzcvKbeAqfxhIg2TVmg70bmhAqvrjCe5Ag6VeNxcPwEARg\n" +
      "pu/tWQbW+yKSqLEWk99asZSXOGhZZNyfhrDNEVpfzVnOEC18D+VqpW+uo3/47oJz\n" +
      "WTrXiRUZAgMBAAECggEAd/XnkMwJKr7law6IaqmqJyzHchmfIty6JH6+yCPJ/U8U\n" +
      "bvMAGMaHeQi8xLtFfQXrSjHcmfg1AWHx91cWzS9+RtfU4qNvhI+f8K31nT1jKBGo\n" +
      "5ETDcHGCOlmbJcy01z/IiCt+R/tfxaNCITEjSnNnt5igYJxXjXgZYhxtJ1DWfNvJ\n" +
      "0PEPRF+Wuo1y77EW4ACzAKdZriLuJ+ynakYIZc8TC+w2ZiSDHx0ZEQ2YmV7m7A11\n" +
      "db1bAw4X7Z7ECvx9VaZKmvj3v9xa8BIcJtYw8YJSHL0f3g/xz+qAqDVw5X7zofdO\n" +
      "FEwx96/o14lWnohVjruaGEQvMsNKNz/ONQ1cUaTsFQKBgQDlik5hjo7AEx8mlu5q\n" +
      "vXt9iAqSSSI56bunJofbfRLihI9Z3e10v5pvdj9CwB/gJYtOJ4rYLo4KGY7AMq7r\n" +
      "q4ObVGF3lIIdBQldy0B+w2jHaemNlg5shQKqC4dA26MlOKO0iIvS1HIazkRGk1sg\n" +
      "p8NVaG7jMvGyzD75UsdPRFoBxwKBgQDJ838J+R2OjAdbcDKblfpc/O+IU5chxNsw\n" +
      "ojlA4NLbROr9RBNj04r565g1kU0vbj14Cj9Ocifb9yZNQJPIPfyVVg8LIrt9YciC\n" +
      "GnvUSdX/558+O7Y/HwqXlzGt3TRnHpdH34qO/CSUEl5kDP/TmW3sw6VNYZa1QwhW\n" +
      "VldKgWYyHwKBgDzUomEIPpx4dNDtPtHa1Vc3LlYGO6PNZYWumGJ6iv6s0rCmN7+w\n" +
      "52SSmcE+2TO1v20+3XTdIZdbnpEg3WpnUcFgY1QlbzXxl8Hbk4QElUgDsXlsQvZP\n" +
      "aZ1W4Mk3a8z5bajyZtvAoVypPT7W3leRHhsMSha78YHIzweUAG3pV1ERAoGATEg4\n" +
      "nVjG7FhCUyyvQQvGtScph3IjrTLBpL4yKCqEGyUOKjpzpIp8fWibZuiKojbe6x/b\n" +
      "x9Lg8XqKsjWJXOLlLLeEGS22ambsKRC943M8bVxdT1GYxoEALECFGGps5+KrPA/Z\n" +
      "M6dUXcYOd3Zdj9zto7hHEVKibbdzR8F3WYJFSvsCgYBj1TqNONCQRJvo298bJtm9\n" +
      "RXzYQvjHXnNNnbEL+B+B/r2jBOM/t8WBLJVNghQDhJY+DMMdptxkvNge3XNenDtY\n" +
      "25UsnB7XefRE0tDe6yLWKbONT33+WGZjCBCXpQ90Avvi9npFetwG9Q8GSP7VdbTc\n" +
      "tLwfZra8DDXs5Dz9Gion+A==\n";

  private String expectedPublicKeyFp2 =
      "SHA256:MDcEdlQsgzIs7UBLHV6CB9GJLqqW/AqGsMcAPrWVxuA=";

  private String storedPublicKey2 =
      "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwwtuB1ZFqe9jF8TjvwoH\n" +
      "oGO2KRxSu8GRDuI93/g1dJKve/SsmNB+SPQ402tEmnejg6QMlyFOvh+bbEqYooXN\n" +
      "6lCaFSk7DAx7aNqr1yU/Jpwzbal0H74PhOAw4u1iyBCf08r8aQHLYkOUF2DcggUI\n" +
      "WCKrBnpEC6vK8aZRWGwpgXB46CkousWXrmKBqbEBJusj2/Fgrk2CZ/OGY/vlzh6A\n" +
      "7TpucviZUF3bsmsEs//63XpTSQsL785uixJbnQye8HDN4iyjvK09dHruIfSVPZ2N\n" +
      "7xPdw7Nvyf+gRBmu2HWCPFpOc7a0XPNarlQPXPLbGz47dIZNEW+8p2jdw1D2PZ3h\n" +
      "vwIDAQAB\n";

  private String storedPrivateKey2 =
      "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDDC24HVkWp72MX\n" +
      "xOO/CgegY7YpHFK7wZEO4j3f+DV0kq979KyY0H5I9DjTa0Sad6ODpAyXIU6+H5ts\n" +
      "Spiihc3qUJoVKTsMDHto2qvXJT8mnDNtqXQfvg+E4DDi7WLIEJ/TyvxpActiQ5QX\n" +
      "YNyCBQhYIqsGekQLq8rxplFYbCmBcHjoKSi6xZeuYoGpsQEm6yPb8WCuTYJn84Zj\n" +
      "++XOHoDtOm5y+JlQXduyawSz//rdelNJCwvvzm6LEludDJ7wcM3iLKO8rT10eu4h\n" +
      "9JU9nY3vE93Ds2/J/6BEGa7YdYI8Wk5ztrRc81quVA9c8tsbPjt0hk0Rb7ynaN3D\n" +
      "UPY9neG/AgMBAAECggEBAL9JLGXBtJzPDC45iOrJWWVxpSt4faNqWWtxcyF++l4T\n" +
      "pks5UTSl9dRywHCImUWs5A6tCzQFFIbd1L5GAqAR/js5RYRPZXuRmk7hdvqPqvmg\n" +
      "48c/E4Y2Dl5QyWElU2XG+Bjs0NPjUKZUhJ7Q/jH94YsepQC7VJTlrSmF5e2EVsh7\n" +
      "3wwAd+fRNSvWmMoVsjw1gX8Cen8rCEZZyPSwaK1pYBQga0abEZ6BdQQ0O3yTZQga\n" +
      "Q3z+vC+hm3cqpAmhWupYxqdA/EBNz/v0GVe8BGbEYAnm/4LPpQilt1Z1ngNqdUoT\n" +
      "zF4PXnYQ/fFOR90nbjWoJLJYCDd+QW4XXwxwOIs/BsECgYEA+USiF2F+lG7y6dhk\n" +
      "fLAi8UROAjYv+/wWSBnOV0JCD0A6Ik5bOXvXs72/NCHdBGq93mbYhzo7ar9Fm0QE\n" +
      "mtwfnfhVNRk+SSq/+hzCOmZrdZpu2nMzBDJtR2TartOwfijE/NNZGA1V7QOmR02u\n" +
      "WINyaqvDNNPVIt+D6qreXcZDslECgYEAyE/pWfLjJfWBXtOruXxBcJKq5Arx0YSv\n" +
      "i7VaiOg9zBtLg4imKvRJqjs6Y7rGap3qA+HguEl4TQHvhIfhlAxWJ2XVB0ijTAiF\n" +
      "aMTmBmxCJq0V9raScJPbSHAN54T2f0x+yh5/q7IFFKF2Hn3qrmarSAw02G+KTH6M\n" +
      "lFFqJ+Dcvw8CgYEA7aMX4LBqq3nGjVdmHVUSSu7ya7tbLaDbYStxAtFBBycVBQWs\n" +
      "hHXjYxD/SuUJvx9AGdn0jZ7fbFojMu26ciRu4/wOx4tkTP67fOeT53ci9UAgdJQk\n" +
      "y9iDQ/ALZ2abOPsHKX0X0A1OoKG9EPcmwm22U6midSeKZy+tpLf3PHE6srECgYAa\n" +
      "yj0+T3K7t+r2gL69zvV9ldAPMbuHtwQ3XijemJjzPE9MJzF6GzPi9Yronak9xyLu\n" +
      "I/6HByR0wCaFhhrQTxoSqNbl43wbhiQ5j+PnxgDO5WVDmsVZEx1HwdzKMwk4m0V1\n" +
      "yMBweR2e1b1TdKm3a3nK5/8FV12av24TxBO7g6JiVwKBgDnaedWvgJt+tJrkFn9h\n" +
      "ZE2VVGC4FjFDpCMxIebvBz5Kbs+lknpdcda8+DMwXFDf8OR3lt/3KzJqleUhrTTz\n" +
      "AQz56Xdi9VEnVs3rsgvX9VnaWcRpa4GT5EIj+I2M9t+D8XCfMMs1S56Pnn5oGkqv\n" +
      "FBzmMRnskqK6d75B8EG5BGi0\n";

  @Test
  public void validatePublicKeyFp()
      throws NoSuchAlgorithmException, InvalidKeySpecException
  {

    PublicKey pubKey = loadPublicKey(storedPublicKey);
    PrivateKey priKey = loadPrivateKey(storedPrivateKey);

    KeyPair keypair = new KeyPair(pubKey, priKey);

    String accountName = "accountName";
    String userName = "userName";
    SecurityManager securityManager =
        new SecurityManager(accountName, userName, keypair);
    String publicKeyFp = securityManager.getPublicKeyFingerPrint();
    assertTrue(publicKeyFp.equals(expectedPublicKeyFp));

    PublicKey pubKey2 = loadPublicKey(storedPublicKey2);
    PrivateKey priKey2 = loadPrivateKey(storedPrivateKey2);

    KeyPair keypair2 = new KeyPair(pubKey2, priKey2);

    SecurityManager securityManager2 =
        new SecurityManager(accountName, userName, keypair2);
    String publicKeyFp2 = securityManager2.getPublicKeyFingerPrint();
    assertTrue(publicKeyFp2.equals(expectedPublicKeyFp2));
  }

  @Test
  public void testParseAccount() throws NoSuchAlgorithmException, InvalidKeySpecException
  {
    PublicKey pubKey = loadPublicKey(storedPublicKey);
    PrivateKey priKey = loadPrivateKey(storedPrivateKey);

    KeyPair keypair = new KeyPair(pubKey, priKey);

    String accountName = "accountName";
    String userName = "userName";
    SecurityManager securityManager =
            new SecurityManager(accountName, userName, keypair);
    Assert.assertEquals(accountName.toUpperCase(), securityManager.getAccount());
  }

  @Test
  public void testParseAccount_dotInAccountName() throws NoSuchAlgorithmException, InvalidKeySpecException
  {
    PublicKey pubKey = loadPublicKey(storedPublicKey);
    PrivateKey priKey = loadPrivateKey(storedPrivateKey);

    KeyPair keypair = new KeyPair(pubKey, priKey);

    String accountName = "accountName.extra";
    String userName = "userName";
    String trimmedAccountName = "accountName";
    SecurityManager securityManager =
            new SecurityManager(accountName, userName, keypair);
    Assert.assertEquals(trimmedAccountName.toUpperCase(), securityManager.getAccount());
  }

  /**
   * Converts encodedBase64 publicKey back to the RSA scheme PublicKey object.
   * <p>
   * @param base64PublicKey
   * @return
   * @throws NoSuchAlgorithmException
   * @throws InvalidKeySpecException
   */
  private PublicKey loadPublicKey(String base64PublicKey)
      throws NoSuchAlgorithmException, InvalidKeySpecException
  {
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
    throws NoSuchAlgorithmException, InvalidKeySpecException
  {
    byte[] clear = Base64.getMimeDecoder().decode(base64PrivateKey);
    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(clear);
    KeyFactory factory = KeyFactory.getInstance("RSA");
    PrivateKey privateKey = factory.generatePrivate(keySpec);
    Arrays.fill(clear, (byte) 0);
    return privateKey;
  }

}
