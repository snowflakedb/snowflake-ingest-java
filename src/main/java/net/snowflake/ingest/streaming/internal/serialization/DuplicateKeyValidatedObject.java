/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.serialization;

/**
 * A wrapper for an Object that is going to be validated by {@link
 * DuplicateKeyValidatingSerializer}.
 */
public class DuplicateKeyValidatedObject {
  private final Object object;

  public DuplicateKeyValidatedObject(Object object) {
    this.object = object;
  }

  public Object getObject() {
    return object;
  }
}
