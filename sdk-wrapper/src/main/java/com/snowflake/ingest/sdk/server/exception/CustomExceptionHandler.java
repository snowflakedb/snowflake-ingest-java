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

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import com.snowflake.ingest.sdk.server.exception.AppServerException;
import com.snowflake.ingest.sdk.server.response.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom exception handler for managing unhandled exceptions in services. Exceptions are logged and
 * a generic error response is returned.
 */
public class CustomExceptionHandler implements ExceptionHandlerFunction {
  private static final Logger logger = LoggerFactory.getLogger(CustomExceptionHandler.class);

  @Override
  public HttpResponse handleException(
      ServiceRequestContext ctx, HttpRequest req, Throwable cause) {
    logger.error(
        "Unhandled exception in {}, path params: {}, query params: {}, error: {}",
        ctx.path(),
        ctx.pathParams(),
        ctx.queryParams().toQueryString(),
        cause.getMessage(),
        cause);

    HttpStatus status = HttpStatus.INTERNAL_SERVER_ERROR;
    if (cause instanceof AppServerException) {
      status = HttpStatus.valueOf(((AppServerException) cause).getHttpStatusCode());
    }
    ErrorResponse errorResponse = ErrorResponse.fromException((AppServerException) cause);

    return HttpResponse.ofJson(status, errorResponse);
  }
}
