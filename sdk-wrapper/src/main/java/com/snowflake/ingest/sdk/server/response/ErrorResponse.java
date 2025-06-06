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

package com.snowflake.ingest.sdk.server.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.snowflake.ingest.sdk.server.exception.AppServerException;

/** Response model for error cases */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ErrorResponse {
  private String message;
  private String errorCode;
  private int httpStatusCode;

  public ErrorResponse() {}

  public ErrorResponse(String message, String errorCode, int httpStatusCode) {
    this.message = message;
    this.errorCode = errorCode;
    this.httpStatusCode = httpStatusCode;
  }

  public static ErrorResponse fromException(AppServerException e) {
    return new ErrorResponse(e.getMessage(), e.getErrorCode().name(), e.getHttpStatusCode());
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  public int getHttpStatusCode() {
    return httpStatusCode;
  }

  public void setHttpStatusCode(int httpStatusCode) {
    this.httpStatusCode = httpStatusCode;
  }
}
