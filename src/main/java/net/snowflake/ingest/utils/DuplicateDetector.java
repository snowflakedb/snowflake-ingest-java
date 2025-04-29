/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import java.util.HashSet;
import java.util.Set;

/**
 * A utility class that detects duplicate objects. Optimized for Json objects with a small number of
 * keys.
 */
public class DuplicateDetector<T> {
  private T firstKey;
  private T secondKey;
  private Set<T> keys;

  public boolean isDuplicate(T key) {
    if (firstKey == null) {
      firstKey = key;
      return false;
    }
    if (firstKey.equals(key)) {
      return true;
    }
    if (secondKey == null) {
      secondKey = key;
      return false;
    }
    if (secondKey.equals(key)) {
      return true;
    }

    if (keys == null) {
      keys = new HashSet<>();
    }
    return !keys.add(key);
  }
}
