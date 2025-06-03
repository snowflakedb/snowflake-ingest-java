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
import com.snowflake.ingest.common.exception.AppServerException;
import com.snowflake.ingest.common.response.ErrorResponse;
import net.snowflake.ingest.utils.SFException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Custom exception handler for the service. */
public class CustomExceptionHandler implements ExceptionHandlerFunction {
  private static final Logger logger = LoggerFactory.getLogger(CustomExceptionHandler.class);

  @Override
  public HttpResponse handleException(ServiceRequestContext ctx, HttpRequest req, Throwable cause) {
    if (cause instanceof AppServerException) {
      AppServerException ase = (AppServerException) cause;
      logger.error("AppServerException occurred", ase);
      return HttpResponse.ofJson(
          HttpStatus.valueOf(ase.getHttpStatusCode()), ErrorResponse.fromException(ase));
    }

    if (cause instanceof SFException) {
      SFException sfe = (SFException) cause;
      logger.error("SFException occurred", sfe);
      return HttpResponse.ofJson(
          HttpStatus.INTERNAL_SERVER_ERROR, ErrorResponse.fromException(sfe));
    }

    logger.error("Unexpected exception occurred", cause);
    return HttpResponse.ofJson(
        HttpStatus.INTERNAL_SERVER_ERROR, ErrorResponse.fromException(cause));
  }
}
