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

package com.snowflake.ingest.sdk.server.exception;

/** Exception thrown by AppServer */
public class AppServerException extends RuntimeException {
  public enum AppErrorCode {
    INVALID_INPUT,
    UNEXPECTED_ERROR,
    TEST_FAILURE
  }

  private int httpStatusCode;
  private final AppErrorCode errorCodeName;

  public AppServerException(AppErrorCode errorCodeName, String message) {
    super(message);
    this.httpStatusCode = 500;
    this.errorCodeName = errorCodeName;
  }

  public AppServerException(AppErrorCode errorCodeName, Throwable cause) {
    super(cause);
    this.errorCodeName = errorCodeName;
  }

  public AppServerException(int httpStatusCode, AppErrorCode errorCodeName, String message) {
    super(message);
    this.httpStatusCode = httpStatusCode;
    this.errorCodeName = errorCodeName;
  }

  public int getHttpStatusCode() {
    return httpStatusCode;
  }

  public AppErrorCode getErrorCode() {
    return errorCodeName;
  }
}
