/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

import static net.snowflake.ingest.utils.Utils.concatDotPath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/** Helper class to find all leaf sub-columns in an immutable schema given a fieldId. */
public class SubColumnFinder {
  static class SubtreeInfo {
    final int startTag;
    final int endTag;
    final String dotPath;

    SubtreeInfo(int startTag, int endTag, String dotPath) {
      this.startTag = startTag;
      this.endTag = endTag;
      this.dotPath = dotPath;
    }
  }

  private final List<String> list;
  private final Map<String, SubtreeInfo> accessMap;

  public SubColumnFinder(MessageType schema) {
    accessMap = new HashMap<>();
    list = new ArrayList<>();
    build(schema, null);
  }

  public List<String> getSubColumns(String id) {
    if (!accessMap.containsKey(id)) {
      throw new IllegalArgumentException(String.format("Field %s not found in schema", id));
    }
    SubtreeInfo interval = accessMap.get(id);
    return Collections.unmodifiableList(list.subList(interval.startTag, interval.endTag));
  }

  public String getDotPath(String id) {
    if (!accessMap.containsKey(id)) {
      throw new IllegalArgumentException(String.format("Field %s not found in schema", id));
    }
    return accessMap.get(id).dotPath;
  }

  private void build(Type node, List<String> path) {
    if (path == null) {
      /* Ignore root node type name (bdec or schema) */
      path = new ArrayList<>();
    } else {
      path.add(node.getName());
    }

    int startTag = list.size();
    if (!node.isPrimitive()) {
      for (Type child : node.asGroupType().getFields()) {
        build(child, path);
      }
    } else {
      list.add(node.getId().toString());
    }
    if (!path.isEmpty() && node.getId() != null) {
      accessMap.put(
          node.getId().toString(),
          new SubtreeInfo(startTag, list.size(), concatDotPath(path.toArray(new String[0]))));
    }
    if (!path.isEmpty()) {
      path.remove(path.size() - 1);
    }
  }
}
