/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import static net.snowflake.ingest.utils.Utils.concatDotPath;
import static net.snowflake.ingest.utils.Utils.isNullOrEmpty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/** Helper class to find all leaf sub-columns in an immutable schema given a dot path. */
public class SubColumnFinder {
  static class SubtreeInterval {
    final int startTag;
    final int endTag;

    SubtreeInterval(int startTag, int endTag) {
      this.startTag = startTag;
      this.endTag = endTag;
    }
  }

  private final List<String> list;
  private final Map<String, SubtreeInterval> accessMap;

  public SubColumnFinder(MessageType schema) {
    accessMap = new HashMap<>();
    list = new ArrayList<>();
    build(schema, null);
  }

  public List<String> getSubColumns(String dotPath) {
    if (!accessMap.containsKey(dotPath)) {
      throw new IllegalArgumentException(String.format("Column %s not found in schema", dotPath));
    }
    SubtreeInterval interval = accessMap.get(dotPath);
    return Collections.unmodifiableList(list.subList(interval.startTag, interval.endTag));
  }

  private void build(Type node, String dotPath) {
    if (dotPath == null) {
      /* Ignore root node type name (bdec or schema) */
      dotPath = "";
    } else if (dotPath.isEmpty()) {
      dotPath = node.getName();
    } else {
      dotPath = concatDotPath(dotPath, node.getName());
    }

    int startTag = list.size();
    if (!node.isPrimitive()) {
      for (Type child : node.asGroupType().getFields()) {
        build(child, dotPath);
      }
    } else {
      list.add(dotPath);
    }
    if (!isNullOrEmpty(dotPath)) {
      accessMap.put(dotPath, new SubtreeInterval(startTag, list.size()));
    }
  }
}
