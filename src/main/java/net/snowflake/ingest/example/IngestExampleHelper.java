/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.example;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper methods to connect to snowflake jdbc and run queries Created by vganesh on 5/19/17. */
public class IngestExampleHelper {
  // a logger for all of our needs in this class
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestExampleHelper.class.getName());
  /**
   * Create directories if they don't exist under directoryPath.
   *
   * @param directoryPath
   * @throws IOException
   */
  public static void makeLocalDirectory(String directoryPath) throws IOException {
    Files.createDirectories(Paths.get(directoryPath));
  }

  public static Path createTempCsv(String directoryPath, String filename, int rows)
      throws IOException {
    final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    final Path csv = Files.createTempFile(Paths.get(directoryPath), filename, ".csv");
    final String fileName = csv.getFileName().toString();

    try (Writer w = Files.newBufferedWriter(csv, StandardCharsets.UTF_8)) {
      for (int i = 0; i < rows; i++) {
        w.write(String.valueOf(i));
        w.write(",");
        w.write("row-");
        w.write(String.valueOf(i));
        w.write(",");
        w.write(String.valueOf(rnd.nextLong()));
        w.write(",");
        w.write(fileName);
        w.write("\n");
      }
    }
    return csv;
  }

  /**
   * Create file under directoryPath with fileName and populate it with test data
   *
   * @param directoryPath
   * @param filename
   * @throws IOException
   */
  public static void makeSampleFile(String directoryPath, String filename) throws IOException {

    File file = new File(directoryPath, filename);

    // if our file doesn't already exist
    if (!file.exists()) {
      // create it
      file.createNewFile();

      // populate it with some data
      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      for (char letter = 'a'; letter <= 'z'; letter++) {
        bw.write(letter + "\n");
      }
      // close it back up
      bw.close();
    }

    return;
  }

  public static Connection getConnection(
      String user, String password, String account, String host, int port) throws Exception {
    // check first to see if we have the Snowflake JDBC
    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");

    // build our properties
    Properties props = new Properties();
    props.put("user", user);
    props.put("password", password);
    props.put("account", account);
    props.put("ssl", "off");

    // the actual connection string
    String connectString = "jdbc:snowflake://" + host + ":" + port;

    // fire off the connection
    return DriverManager.getConnection(connectString, props);
  }

  /**
   * Try to execute a SQL query and throw if it fails
   *
   * @param query the query in question
   */
  public static void doQuery(Connection conn, String query) {
    LOGGER.info("doQuery {}", query);
    try (Statement statement = conn.createStatement()) {
      statement.executeQuery(query);
    }
    // if ANY exceptions occur, an illegal state has been reached
    catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static String ALGORITHM = "RSA";
  /**
   * Generates an RSA keypair for use in this test
   *
   * @return a valid RSA keypair
   * @throws NoSuchAlgorithmException if we don't have an RSA algo
   * @throws NoSuchProviderException if we can't use SHA1PRNG for randomization
   */
  public static KeyPair generateKeyPair() throws NoSuchProviderException, NoSuchAlgorithmException {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance(ALGORITHM);
    SecureRandom random = SecureRandom.getInstance("SHA1PRNG", "SUN");
    keyGen.initialize(2048, random);
    return keyGen.generateKeyPair();
  }

  /**
   * Generate the public key as a string
   *
   * @return the public key as a string
   */
  public static String getPublicKeyString(KeyPair keypair)
      throws NoSuchAlgorithmException, InvalidKeySpecException {
    KeyFactory keyFactory = KeyFactory.getInstance(ALGORITHM);
    final PublicKey pk = keypair.getPublic();
    X509EncodedKeySpec spec = keyFactory.getKeySpec(pk, X509EncodedKeySpec.class);
    return Base64.encodeBase64String(spec.getEncoded());
  }
}
