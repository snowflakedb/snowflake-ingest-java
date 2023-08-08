/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.client.core.SFBaseSession;

import java.util.List;

/**
 * An interface to use for returning query results from any java class
 *
 * @author jhuang
 */
public interface SnowflakeFixedView {
  List<SnowflakeColumnMetadata> describeColumns(SFBaseSession session) throws Exception;

  List<Object> getNextRow() throws Exception;

  int getTotalRows();
}
