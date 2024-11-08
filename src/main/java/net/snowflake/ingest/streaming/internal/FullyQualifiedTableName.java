package net.snowflake.ingest.streaming.internal;

import java.util.Objects;
import net.snowflake.ingest.utils.Utils;

/**
 * FullyQualifiedTableName is a class that represents a fully qualified table name. It is used to
 * store the fully qualified table name in the Snowflake format.
 */
public class FullyQualifiedTableName {
  public FullyQualifiedTableName(String databaseName, String schemaName, String tableName) {
    this.databaseName = databaseName;
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  // Database name
  private final String databaseName;

  // Schema name
  private final String schemaName;

  // Table Name
  private final String tableName;

  public String getTableName() {
    return tableName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getFullyQualifiedName() {
    return Utils.getFullyQualifiedTableName(databaseName, schemaName, tableName);
  }

  private int hashCode;

  @Override
  public int hashCode() {
    int result = hashCode;
    if (result == 0) {
      result = 31 + ((databaseName == null) ? 0 : databaseName.hashCode());
      result = 31 * result + ((schemaName == null) ? 0 : schemaName.hashCode());
      result = 31 * result + ((tableName == null) ? 0 : tableName.hashCode());
      hashCode = result;
    }

    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;

    if (!(obj instanceof FullyQualifiedTableName)) return false;

    FullyQualifiedTableName other = (FullyQualifiedTableName) obj;

    if (!Objects.equals(databaseName, other.databaseName)) return false;
    if (!Objects.equals(schemaName, other.schemaName)) return false;
    return Objects.equals(tableName, other.tableName);
  }
}
