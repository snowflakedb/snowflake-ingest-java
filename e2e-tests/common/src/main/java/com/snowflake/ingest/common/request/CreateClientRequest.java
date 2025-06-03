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

/** POJO representing create client request for Streaming Ingest */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateClientRequest {
  private String clientId;
  private Map<String, String> parameterOverrides;

  public CreateClientRequest() {}

  public CreateClientRequest(
      String clientId,
      Map<String, String> parameterOverrides) {
    this.clientId = clientId;
    this.parameterOverrides = parameterOverrides;
  }

  // Getters and Setters
  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public Map<String, String> getParameterOverrides() {
    return parameterOverrides;
  }

  public void setParameterOverrides(Map<String, String> parameterOverrides) {
    this.parameterOverrides = parameterOverrides;
  }

  @Override
  public String toString() {
    return "CreateClientRequest{"
        + "clientId='"
        + clientId
        + '\''
        + ", parameterOverrides="
        + parameterOverrides
        + '}';
  }
}
