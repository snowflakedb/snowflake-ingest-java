/*
 * Replicated from snowflake-jdbc: net.snowflake.client.core.FileUtil
 * Tag: v3.25.1
 * Source: https://github.com/snowflakedb/snowflake-jdbc/blob/v3.25.1/src/main/java/net/snowflake/client/core/FileUtil.java
 *
 * Only the methods used by FileBackedOutputStream are included.
 * Permitted differences: package line, SnowflakeUtil.isWindows/isNullOrEmpty
 * delegated to StorageClientUtil (replicated from SnowflakeUtil).
 */

package net.snowflake.ingest.streaming.internal.fileTransferAgent;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Arrays;
import java.util.Collection;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLogger;
import net.snowflake.ingest.streaming.internal.fileTransferAgent.log.SFLoggerFactory;

class FileUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(FileUtil.class);
  private static final Collection<PosixFilePermission> WRITE_BY_OTHERS =
      Arrays.asList(PosixFilePermission.GROUP_WRITE, PosixFilePermission.OTHERS_WRITE);
  private static final Collection<PosixFilePermission> READ_BY_OTHERS =
      Arrays.asList(PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_READ);
  private static final Collection<PosixFilePermission> EXECUTABLE =
      Arrays.asList(
          PosixFilePermission.OWNER_EXECUTE,
          PosixFilePermission.GROUP_EXECUTE,
          PosixFilePermission.OTHERS_EXECUTE);

  public static void logFileUsage(Path filePath, String context, boolean logReadAccess) {
    logWarnWhenAccessibleByOthers(filePath, context, logReadAccess);
  }

  public static void logFileUsage(File file, String context, boolean logReadAccess) {
    logFileUsage(file.toPath(), context, logReadAccess);
  }

  private static void logWarnWhenAccessibleByOthers(
      Path filePath, String context, boolean logReadAccess) {
    // we do not check the permissions for Windows
    if (isWindows()) {
      return;
    }

    try {
      Collection<PosixFilePermission> filePermissions = Files.getPosixFilePermissions(filePath);
      logger.debug(
          "{}File {} access rights: {}", getContextStr(context), filePath, filePermissions);

      boolean isWritableByOthers = isPermPresent(filePermissions, WRITE_BY_OTHERS);
      boolean isReadableByOthers = isPermPresent(filePermissions, READ_BY_OTHERS);
      boolean isExecutable = isPermPresent(filePermissions, EXECUTABLE);

      if (isWritableByOthers || (isReadableByOthers && logReadAccess) || isExecutable) {
        logger.warn(
            "{}File {} is accessible by others to:{}{}",
            getContextStr(context),
            filePath,
            isReadableByOthers && logReadAccess ? " read" : "",
            isWritableByOthers ? " write" : "",
            isExecutable ? " execute" : "");
      }
    } catch (IOException e) {
      logger.warn(
          "{}Unable to access the file to check the permissions: {}. Error: {}",
          getContextStr(context),
          filePath,
          e);
    }
  }

  private static boolean isPermPresent(
      Collection<PosixFilePermission> filePerms, Collection<PosixFilePermission> permsToCheck)
      throws IOException {
    return filePerms.stream().anyMatch(permsToCheck::contains);
  }

  private static String getContextStr(String context) {
    return StorageClientUtil.isNullOrEmpty(context) ? "" : context + ": ";
  }

  private static boolean isWindows() {
    return StorageClientUtil.isWindows();
  }
}
