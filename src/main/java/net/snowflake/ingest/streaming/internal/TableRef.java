package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.utils.Utils;

import javax.annotation.Nonnull;

class TableRef {
    final String dbName;
    final String schemaName;
    final String tableName;
    final String fullyQualifiedName;

    TableRef(@Nonnull String dbName, @Nonnull String schemaName, @Nonnull String tableName) {
        this.dbName = dbName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.fullyQualifiedName = Utils.getFullyQualifiedTableName(dbName, schemaName, tableName);
    }

    @Override
    public int hashCode() {
        return dbName.hashCode() ^ schemaName.hashCode() ^ tableName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (this == obj) {
            return true;
        }

        if (TableRef.class != obj.getClass()) {
            return false;
        }

        final TableRef other = (TableRef) obj;
        return this.dbName.equals(other.dbName) && this.schemaName.equals(other.schemaName) && this.tableName.equals(other.tableName);
    }

    @Override
    public String toString() {
        return this.fullyQualifiedName;
    }
}
