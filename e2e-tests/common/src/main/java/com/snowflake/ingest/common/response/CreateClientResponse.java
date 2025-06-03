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

package com.snowflake.ingest.common.response;

/** Class represents response on creating a client on the App server */
public class CreateClientResponse {

  /** Uniquely identifies a client on the App server */
  private String clientId;

  public CreateClientResponse() {}

  private CreateClientResponse(String clientId) {
    this.clientId = clientId;
  }

  public static CreateClientResponse of(String clientId) {
    return new CreateClientResponse(clientId);
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }
}
