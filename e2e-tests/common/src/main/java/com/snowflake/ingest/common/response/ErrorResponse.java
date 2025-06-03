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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.common.exception.AppServerException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.ErrorCode;
import java.io.IOException;

/** Class represents error response from the App server */
public class ErrorResponse {

  public enum ErrorSource {
    APP_SERVER,
    SDK
  }

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Enum to represent the source of the error. Errors that originate from SSV2 SDK are of type SDK
   * and errors that originate from the App server are of type APP_SERVER
   */
  private ErrorSource errorSource;

  private String errorCodeName;
  private String message;
  private String correlationId;

  private ErrorResponse() {}

  private ErrorResponse(
      ErrorSource errorSource, String errorCodeName, String message, String correlationId) {
    this.errorSource = errorSource;
    this.errorCodeName = errorCodeName;
    this.message = message;
    this.correlationId = correlationId;
  }

  public static ErrorResponse of(ErrorSource errorSource, String errorCodeName, String message) {
    return new ErrorResponse(errorSource, errorCodeName, message, null);
  }

  public static ErrorResponse of(
      ErrorSource errorSource, String errorCodeName, String message, String correlationId) {
    return new ErrorResponse(errorSource, errorCodeName, message, correlationId);
  }

  public ErrorSource getErrorSource() {
    return errorSource;
  }

  public void setErrorSource(ErrorSource errorSource) {
    this.errorSource = errorSource;
  }

  public String getErrorCodeName() {
    return errorCodeName;
  }

  public void setErrorCodeName(String errorCodeName) {
    this.errorCodeName = errorCodeName;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getCorrelationId() {
    return correlationId;
  }

  public void setCorrelationId(String correlationId) {
    this.correlationId = correlationId;
  }

  @Override
  public String toString() {
    return "ErrorResponse{"
        + "errorSource="
        + errorSource
        + ", errorCodeName='"
        + errorCodeName
        + '\''
        + ", message='"
        + message
        + '\''
        + ", correlationId='"
        + correlationId
        + '\''
        + '}';
  }

  public static ErrorResponse fromBytes(byte[] data) {
    try {
      return OBJECT_MAPPER.readValue(data, ErrorResponse.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize error response", e);
    }
  }

  public RuntimeException toException() {
    if (ErrorSource.SDK.equals(this.errorSource)) {
      return new SFException(ErrorCode.valueOf(errorCodeName), message, correlationId);
    }
    return new AppServerException(AppServerException.AppErrorCode.valueOf(errorCodeName), message);
  }

  public static ErrorResponse fromException(Throwable cause) {
    if (cause instanceof SFException) {
      SFException sfException = (SFException) cause;
      return ErrorResponse.of(
          ErrorSource.SDK,
          sfException.getVendorCode(),
          sfException.getMessage(),
          null);
    }
    if (cause instanceof AppServerException) {
      AppServerException appServerException = (AppServerException) cause;
      return ErrorResponse.of(
          ErrorSource.APP_SERVER,
          appServerException.getErrorCode().name(),
          appServerException.getMessage());
    }
    return ErrorResponse.of(ErrorSource.APP_SERVER, "UNEXPECTED_ERROR", cause.getMessage());
  }
}
