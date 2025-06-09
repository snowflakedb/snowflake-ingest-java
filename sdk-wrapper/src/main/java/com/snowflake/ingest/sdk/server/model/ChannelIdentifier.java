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

package com.snowflake.ingest.sdk.server.model;

import java.util.Objects;

/** Class to represent a channel identifier. This is used to uniquely identify a channel. */
public class ChannelIdentifier {
  private final String clientId;
  private final String channelName;

  private ChannelIdentifier(String clientId, String channelName) {
    this.clientId = clientId;
    this.channelName = channelName;
  }

  public static ChannelIdentifier of(String clientId, String channelName) {
    return new ChannelIdentifier(clientId, channelName);
  }

  public String getClientId() {
    return clientId;
  }

  public String getChannelName() {
    return channelName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ChannelIdentifier that = (ChannelIdentifier) o;
    return Objects.equals(clientId, that.clientId) && Objects.equals(channelName, that.channelName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(clientId, channelName);
  }

  @Override
  public String toString() {
    return "ChannelIdentifier{"
        + "clientId='"
        + clientId
        + '\''
        + ", channelName='"
        + channelName
        + '\''
        + '}';
  }
}
