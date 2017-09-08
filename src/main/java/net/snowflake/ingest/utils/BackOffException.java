/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

/**
 * BackOffException - Exception thrown when we have to back off
 *
 * @author obabarinsa
 */
public class BackOffException extends RuntimeException
{
  public BackOffException()
  {
    super();
  }

  public BackOffException(String message)
  {
    super(message);
  }

  public BackOffException(String message, Throwable cause)
  {
    super(message, cause);
  }

  public BackOffException(Throwable cause)
  {
    super(cause);
  }
}
