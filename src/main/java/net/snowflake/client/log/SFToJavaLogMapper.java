package net.snowflake.client.log;

import java.util.HashMap;
import java.util.logging.Level;

/** Utility class to map SFLogLevels to java.util.logging.Level; */
public class SFToJavaLogMapper {
  private static HashMap<SFLogLevel, Level> levelMap = new HashMap<>();

  static {
    levelMap.put(SFLogLevel.TRACE, Level.FINEST);
    levelMap.put(SFLogLevel.DEBUG, Level.FINE);
    levelMap.put(SFLogLevel.INFO, Level.INFO);
    levelMap.put(SFLogLevel.WARN, Level.WARNING);
    levelMap.put(SFLogLevel.ERROR, Level.SEVERE);
    levelMap.put(SFLogLevel.OFF, Level.OFF);
  }

  public static Level toJavaUtilLoggingLevel(SFLogLevel level) {
    return levelMap.getOrDefault(level, Level.OFF);
  }
}
