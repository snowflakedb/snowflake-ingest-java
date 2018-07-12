/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class provides support functions for writing a thread factory wrapper
 */
public class ThreadFactoryUtil
{
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ThreadFactoryUtil.class);

  public static ThreadFactory poolThreadFactory(final String threadBaseName,
                                                final boolean isDaemon)
  {
    return new ThreadFactory()
    {
      final AtomicLong count = new AtomicLong(0);

      @Override
      public Thread newThread(Runnable r)
      {
        final Thread thread = Executors.defaultThreadFactory().newThread(r);
        thread.setName(threadBaseName + "-" + count.incrementAndGet()
                           + "(" + thread.getId() + ")");
        thread.setDaemon(isDaemon);

        final Thread.UncaughtExceptionHandler uncaughtExceptionHandler =
            (t, e) -> LOGGER.error("uncaughtException in thread: " + t, e);

        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return thread;
      }
    };
  }
}

