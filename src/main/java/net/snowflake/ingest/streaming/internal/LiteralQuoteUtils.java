/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.ingest.streaming.internal;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutionException;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;

/**
 * Util class to normalise literals to match server side metadata.
 *
 * <p>Note: The methods in this class have to be kept in sync with the respective methods on server
 * side.
 */
class LiteralQuoteUtils {

  /** Maximum number of unquoted column names to store in cache */
  static final int UNQUOTED_COLUMN_NAME_CACHE_MAX_SIZE = 30000;

  /** Expiration policy for the cache of unquoted column names */
  static final Duration UNQUOTED_COLUMN_NAME_CACHE_EXPIRATION = Duration.of(5, ChronoUnit.MINUTES);

  /** Cache storing unquoted column names */
  private static final LoadingCache<String, String> unquotedColumnNamesCache;

  static {
    unquotedColumnNamesCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(UNQUOTED_COLUMN_NAME_CACHE_EXPIRATION)
            .maximumSize(UNQUOTED_COLUMN_NAME_CACHE_MAX_SIZE)
            .build(
                new CacheLoader<String, String>() {
                  @Override
                  public String load(String key) {
                    return unquoteColumnNameInternal(key);
                  }
                });
  }

  static String unquoteColumnName(String columnName) {
    try {
      return unquotedColumnNamesCache.get(columnName);
    } catch (ExecutionException e) {
      throw new SFException(
          e, ErrorCode.INTERNAL_ERROR, "Exception thrown while unquoting column name");
    }
  }

  /**
   * Unquote SQL literal.
   *
   * <p>Normalises the column name to how it is stored internally. This function needs to keep in
   * sync with server side normalisation. The reason, we do it here, is to store the normalised
   * column name in BDEC file metadata.
   *
   * @param columnName column name literal to unquote
   * @return unquoted literal
   */
  private static String unquoteColumnNameInternal(String columnName) {
    int length = columnName.length();

    if (length == 0) {
      return columnName;
    }

    // If this is an identifier that starts and ends with double quotes,
    // remove them - accounting for escaped double quotes.
    // Differs from the second condition in that this one allows repeated
    // double quotes
    if (columnName.charAt(0) == '"'
        && (length >= 2
            && columnName.charAt(length - 1) == '"'
            &&
            // Condition that the string contains no single double-quotes
            // but allows repeated double-quotes
            !columnName.substring(1, length - 1).replace("\"\"", "").contains("\""))) {
      // Remove quotes and turn escaped double-quotes to single double-quotes
      return columnName.substring(1, length - 1).replace("\"\"", "\"");
    }

    // If this is an identifier that starts and ends with double quotes,
    // remove them. Internal single double-quotes are not allowed.
    else if (columnName.charAt(0) == '"'
        && (length >= 2
            && columnName.charAt(length - 1) == '"'
            && !columnName.substring(1, length - 1).contains("\""))) {
      // Remove the quotes
      return columnName.substring(1, length - 1);
    }

    // unquoted string that can have escaped spaces
    else {
      // replace escaped spaces in unquoted name
      if (columnName.contains("\\ ")) {
        columnName = columnName.replace("\\ ", " ");
      }
      return columnName.toUpperCase();
    }
  }
}
