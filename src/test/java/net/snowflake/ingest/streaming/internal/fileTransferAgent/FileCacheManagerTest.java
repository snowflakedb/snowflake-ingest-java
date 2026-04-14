// Ported from snowflake-jdbc: net.snowflake.client.core.FileCacheManagerTest
package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import static net.snowflake.ingest.streaming.internal.fileTransferAgent.StorageClientUtil.isWindows;
import static net.snowflake.ingest.streaming.internal.fileTransferAgent.StorageClientUtil.systemGetProperty;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.mockito.ArgumentMatchers.isA;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class FileCacheManagerTest {

  private static final ObjectNode MAPPER_NODE =
      ObjectMapperFactory.getObjectMapper().createObjectNode();

  private static final String CACHE_FILE_NAME = "credential_cache_v1.json.json";
  private static final String CACHE_DIR_PROP = "net.snowflake.jdbc.temporaryCredentialCacheDir";
  private static final String CACHE_DIR_ENV = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR";
  private static final long CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS = 60L;

  private FileCacheManager fileCacheManager;
  private File cacheFile;

  @Before
  public void setup() throws IOException {
    fileCacheManager =
        FileCacheManager.builder()
            .setCacheDirectorySystemProperty(CACHE_DIR_PROP)
            .setCacheDirectoryEnvironmentVariable(CACHE_DIR_ENV)
            .setBaseCacheFileName(CACHE_FILE_NAME)
            .setCacheFileLockExpirationInSeconds(CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS)
            .build();
    cacheFile = createCacheFile();
  }

  @After
  public void clean() throws IOException {
    if (Files.exists(cacheFile.toPath())) {
      Files.delete(cacheFile.toPath());
    }
    if (Files.exists(cacheFile.getParentFile().toPath())) {
      Files.delete(cacheFile.getParentFile().toPath());
    }
  }

  private void verifyPermissionBehavior(
      String permission, String parentDirectoryPermissions, boolean isSucceed) throws IOException {
    fileCacheManager.overrideCacheFile(cacheFile);
    Files.setPosixFilePermissions(cacheFile.toPath(), PosixFilePermissions.fromString(permission));
    Files.setPosixFilePermissions(
        cacheFile.getParentFile().toPath(),
        PosixFilePermissions.fromString(parentDirectoryPermissions));
    if (isSucceed) {
      // assertDoesNotThrow equivalent: just call it
      fileCacheManager.readCacheFile();
    } else {
      try {
        fileCacheManager.readCacheFile();
        fail("Expected SecurityException");
      } catch (SecurityException ex) {
        assertTrue(ex.getMessage().contains("is wider than allowed."));
      }
    }
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwx() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwx------", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRw() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rw-------", "rwx------", true);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwAndParentWide() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rw-------", "rwx--xrwx", true);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRx() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("r-x------", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermR() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("r--------", "rwx------", true);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwxRwx() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwxrwx---", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwxRw() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwxrw----", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwxRx() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwxr-x---", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwxR() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwxr-----", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwxWx() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwx-wx---", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwxW() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwx-w----", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwxX() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwx--x---", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwxOtherRwx() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwx---rwx", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwxOtherRw() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwx---rw-", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwxOtherRx() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwx---r-x", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwxOtherR() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwx---r--", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwxOtherWx() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwx----wx", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwxOtherW() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwx----w-", "rwx------", false);
  }

  @Test
  public void throwWhenReadCacheFileWithPermRwxOtherX() throws IOException {
    assumeFalse(isWindows());
    verifyPermissionBehavior("rwx-----x", "rwx------", false);
  }

  @Test
  public void notThrowExceptionWhenCacheFolderIsNotAccessibleWhenReadFromCache()
      throws IOException {
    assumeFalse(isWindows());
    try {
      Files.setPosixFilePermissions(
          cacheFile.getParentFile().toPath(), PosixFilePermissions.fromString("---------"));
      FileCacheManager fcm =
          FileCacheManager.builder()
              .setCacheDirectorySystemProperty(CACHE_DIR_PROP)
              .setCacheDirectoryEnvironmentVariable(CACHE_DIR_ENV)
              .setBaseCacheFileName(CACHE_FILE_NAME)
              .setCacheFileLockExpirationInSeconds(CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS)
              .build();
      // assertDoesNotThrow equivalent
      fcm.readCacheFile();
    } finally {
      Files.setPosixFilePermissions(
          cacheFile.getParentFile().toPath(), PosixFilePermissions.fromString("rwx------"));
    }
  }

  @Test
  public void notThrowExceptionWhenCacheFolderIsNotAccessibleWhenWriteToCache() throws IOException {
    assumeFalse(isWindows());
    String tmpDirPath = System.getProperty("java.io.tmpdir");
    String cacheDirPath = tmpDirPath + File.separator + "snowflake-cache-dir-noaccess";
    System.setProperty("FILE_CACHE_MANAGER_CACHE_PATH", cacheDirPath);
    try {
      Files.createDirectory(
          Paths.get(cacheDirPath),
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("---------")));

      FileCacheManager fcm =
          FileCacheManager.builder()
              .setOnlyOwnerPermissions(false)
              .setCacheDirectorySystemProperty("FILE_CACHE_MANAGER_CACHE_PATH")
              .setCacheDirectoryEnvironmentVariable("NONEXISTENT")
              .setBaseCacheFileName("cache-file")
              .build();
      // assertDoesNotThrow equivalent
      fcm.writeCacheFile(ObjectMapperFactory.getObjectMapper().createObjectNode());
    } finally {
      Files.deleteIfExists(Paths.get(cacheDirPath));
      System.clearProperty("FILE_CACHE_MANAGER_CACHE_PATH");
    }
  }

  @Ignore("Requires mockito-inline for Mockito.mockStatic(); project uses mockito-core + PowerMock")
  @Test
  public void throwWhenOverrideCacheFileHasDifferentOwnerThanCurrentUserTest() {
    assumeFalse(isWindows());
    try (MockedStatic<FileUtil> fileUtilMock =
        Mockito.mockStatic(FileUtil.class, Mockito.CALLS_REAL_METHODS)) {
      fileUtilMock.when(() -> FileUtil.getFileOwnerName(isA(Path.class))).thenReturn("anotherUser");
      try {
        fileCacheManager.readCacheFile();
        fail("Expected SecurityException");
      } catch (SecurityException ex) {
        assertTrue(ex.getMessage().contains("The file owner is different than current user"));
      }
    }
  }

  @Test
  public void notThrowForToWidePermissionsWhenOnlyOwnerPermissionsSetFalseTest()
      throws IOException {
    assumeFalse(isWindows());
    fileCacheManager.setOnlyOwnerPermissions(false);
    Files.setPosixFilePermissions(cacheFile.toPath(), PosixFilePermissions.fromString("rwxrwx---"));
    // assertDoesNotThrow equivalent
    fileCacheManager.readCacheFile();
  }

  @Test
  public void throwWhenOverrideCacheFileNotFound() {
    assumeFalse(isWindows());
    Path wrongPath =
        Paths.get(systemGetProperty("user.home"), ".cache", "snowflake2", "wrongFileName");
    try {
      fileCacheManager.overrideCacheFile(wrongPath.toFile());
      fail("Expected SecurityException");
    } catch (SecurityException ex) {
      assertTrue(
          ex.getMessage()
              .contains(
                  "Unable to access the file/directory to check the permissions. Error:"
                      + " java.nio.file.NoSuchFileException:"));
    }
  }

  @Test
  public void throwWhenSymlinkAsCache() throws IOException {
    assumeFalse(isWindows());
    Path symlink = createSymlink();
    try {
      fileCacheManager.overrideCacheFile(symlink.toFile());
      fail("Expected SecurityException");
    } catch (SecurityException ex) {
      assertTrue(ex.getMessage().contains("Symbolic link is not allowed for file cache"));
    } finally {
      if (Files.exists(symlink)) {
        Files.delete(symlink);
      }
    }
  }

  private File createCacheFile() {
    Path cacheFile =
        Paths.get(systemGetProperty("user.home"), ".cache", "snowflake_cache", CACHE_FILE_NAME);
    try {
      if (Files.exists(cacheFile)) {
        Files.delete(cacheFile);
      }
      if (Files.exists(cacheFile.getParent())) {
        Files.delete(cacheFile.getParent());
      }
      if (!isWindows()) {
        Files.createDirectories(
            cacheFile.getParent(),
            PosixFilePermissions.asFileAttribute(
                Stream.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE,
                        PosixFilePermission.OWNER_EXECUTE)
                    .collect(Collectors.toSet())));
      } else {
        Files.createDirectories(cacheFile.getParent());
      }

      if (!isWindows()) {
        Files.createFile(
            cacheFile,
            PosixFilePermissions.asFileAttribute(
                Stream.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)
                    .collect(Collectors.toSet())));
      } else {
        Files.createFile(cacheFile);
      }
      ObjectNode cacheContent = ObjectMapperFactory.getObjectMapper().createObjectNode();
      cacheContent.put("token", "tokenValue");
      fileCacheManager.overrideCacheFile(cacheFile.toFile());
      fileCacheManager.writeCacheFile(cacheContent);
      return cacheFile.toFile();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Path createSymlink() throws IOException {
    Path link = Paths.get(cacheFile.getParent(), "symlink_" + CACHE_FILE_NAME);
    if (Files.exists(link)) {
      Files.delete(link);
    }
    return Files.createSymbolicLink(link, cacheFile.toPath());
  }

  @Test
  public void shouldCreateDirAndFile() {
    String tmpDirPath = System.getProperty("java.io.tmpdir");
    String cacheDirPath = tmpDirPath + File.separator + "snowflake-cache-dir";
    System.setProperty("FILE_CACHE_MANAGER_SHOULD_CREATE_DIR_AND_FILE", cacheDirPath);
    FileCacheManager.builder()
        .setOnlyOwnerPermissions(false)
        .setCacheDirectorySystemProperty("FILE_CACHE_MANAGER_SHOULD_CREATE_DIR_AND_FILE")
        .setBaseCacheFileName("cache-file")
        .build();
    assertTrue(new File(tmpDirPath).exists());
  }
}
