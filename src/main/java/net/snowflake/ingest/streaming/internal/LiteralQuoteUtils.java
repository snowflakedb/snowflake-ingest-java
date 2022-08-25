package net.snowflake.ingest.streaming.internal;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import net.snowflake.ingest.utils.Utils;

/**
 * Util class to normalise literals to match server side metadata.
 *
 * <p>Note: The methods in this class have to be kept in sync with the respective methods on server
 * side.
 */
class LiteralQuoteUtils {
  private static final String[] reservedKeywords = {
    // ANSI reserved KW
    "ALL",
    "ALTER",
    "AND",
    "ANY",
    "AS",
    "BETWEEN",
    "BY",
    "CHECK",
    "COLUMN",
    "CONNECT",
    "CREATE",
    "CURRENT",
    "DELETE",
    "DISTINCT",
    "DROP",
    "ELSE",
    "EXISTS",
    "FOLLOWING",
    "FOR",
    "FROM",
    "GRANT",
    "GROUP",
    "HAVING",
    "IN",
    "INSERT",
    "INTERSECT",
    "INTO",
    "IS",
    "LIKE",
    "NOT",
    "NULL",
    "OF",
    "ON",
    "OR",
    "ORDER",
    "REVOKE",
    "ROW",
    "ROWS",
    "SAMPLE",
    "SELECT",
    "SET",
    "START",
    "TABLE",
    "TABLESAMPLE",
    "THEN",
    "TO",
    "TRIGGER",
    "UNION",
    "UNIQUE",
    "UPDATE",
    "VALUES",
    "WHENEVER",
    "WHERE",
    "WITH",

    // oracle reserved KW
    "INCREMENT",
    "MINUS",

    // snowflake reserved KW
    "AGGREGATE",
    "ILIKE",
    "REGEXP",
    "RLIKE",
    "SOME",
    "QUALIFY"
  };

  private static final Set<String> reservedKeywordSet =
      new HashSet<>(Arrays.asList(reservedKeywords));

  // column names that do not need quoting to match metadata
  private static final Pattern unquotedIdentifierPattern =
      Pattern.compile("[$][0-9]+|[_A-Z][_A-Z0-9]*[$]?[_A-Z0-9]*");

  /**
   * Format the column name, given with the user inserted row, to match column metadata, sent by
   * server side.
   */
  static String formatColumnName(String columnName) {
    Utils.assertStringNotNullOrEmpty("invalid column name", columnName);
    // internal server side normalised column name in persistent metadata
    String normalisedInternalName = unquoteColumnName(columnName);
    // display name sent by server side to client in open channel column metadata response
    return quoteColumnNameIfNeeded(normalisedInternalName);
  }

  /**
   * Quote user column name to match server side metadata.
   *
   * <p>This needs to be in sync with server side to code, until we decide to send the normalised
   * unquoted column name from server.
   */
  private static String quoteColumnNameIfNeeded(String columnName) {
    if (!unquotedIdentifierPattern.matcher(columnName).matches()
        || reservedKeywordSet.contains(columnName)) {
      columnName = '"' + columnName.replace("\"", "\"\"") + '"';
    }
    return columnName;
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
