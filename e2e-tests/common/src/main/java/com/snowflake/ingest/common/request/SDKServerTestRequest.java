/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.snowflake.ingest.common.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;

/** POJO representing SDK server test request for Streaming Ingest */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SDKServerTestRequest {
  private String clientId;
  private String dbName;
  private String schemaName;
  private String tableName;
  private Map<String, String> properties;

  public SDKServerTestRequest() {}

  public SDKServerTestRequest(
      String clientId,
      String dbName,
      String schemaName,
      String tableName,
      Map<String, String> properties) {
    this.clientId = clientId;
    this.dbName = dbName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.properties = properties;
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public String toString() {
    return "SDKServerTestRequest{"
        + "clientId='"
        + clientId
        + '\''
        + ", dbName='"
        + dbName
        + '\''
        + ", schemaName='"
        + schemaName
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", properties="
        + properties
        + '}';
  }
}
