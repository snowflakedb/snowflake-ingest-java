/*
 * Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.ingest.streaming.internal;

/**
 * Util class to normalise literals to match server side metadata.
 *
 * <p>Note: The methods in this class have to be kept in sync with the respective methods on server
 * side.
 */
class LiteralQuoteUtils {

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
  static String unquoteColumnName(String columnName) {
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
