/*
 * Copyright (c) 2022-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import net.snowflake.ingest.utils.Utils;

/**
 * Channel immutable identification and encryption attributes.
 *
 * <p>It is shared with the {@link ChannelData} and used for BDEC flush as well.
 */
class ChannelFlushContext {
  private final String name;
  private final String fullyQualifiedName;
  private final String dbName;
  private final String schemaName;
  private final String tableName;
  private final String fullyQualifiedTableName;

  // Sequencer for this channel, corresponding to client sequencer at server side because each
  // connection to a channel at server side will be seen as a connection from a new client
  private final Long channelSequencer;

  // Data encryption key
  private final String encryptionKey;

  // Data encryption key id
  private final Long encryptionKeyId;

  ChannelFlushContext(
      String name,
      String dbName,
      String schemaName,
      String tableName,
      Long channelSequencer,
      String encryptionKey,
      Long encryptionKeyId) {
    this.name = name;
    this.fullyQualifiedName =
        Utils.getFullyQualifiedChannelName(dbName, schemaName, tableName, name);
    this.dbName = dbName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.fullyQualifiedTableName = Utils.getFullyQualifiedTableName(dbName, schemaName, tableName);
    this.channelSequencer = channelSequencer;
    this.encryptionKey = encryptionKey;
    this.encryptionKeyId = encryptionKeyId;
  }

  @Override
  public String toString() {
    return "ChannelContext{"
        + "name='"
        + getName()
        + '\''
        + ", fullyQualifiedName='"
        + getFullyQualifiedName()
        + '\''
        + ", dbName='"
        + getDbName()
        + '\''
        + ", schemaName='"
        + getSchemaName()
        + '\''
        + ", tableName='"
        + getTableName()
        + '\''
        + ", fullyQualifiedTableName='"
        + getFullyQualifiedTableName()
        + '\''
        + ", channelSequencer="
        + getChannelSequencer()
        + ", encryptionKey='"
        + getEncryptionKey()
        + '\''
        + ", encryptionKeyId="
        + getEncryptionKeyId()
        + '}';
  }

  String getName() {
    return name;
  }

  String getFullyQualifiedName() {
    return fullyQualifiedName;
  }

  String getDbName() {
    return dbName;
  }

  String getSchemaName() {
    return schemaName;
  }

  String getTableName() {
    return tableName;
  }

  String getFullyQualifiedTableName() {
    return fullyQualifiedTableName;
  }

  Long getChannelSequencer() {
    return channelSequencer;
  }

  String getEncryptionKey() {
    return encryptionKey;
  }

  Long getEncryptionKeyId() {
    return encryptionKeyId;
  }
}
