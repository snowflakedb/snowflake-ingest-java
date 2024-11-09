/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal.it;

import net.snowflake.ingest.IcebergIT;
import net.snowflake.ingest.utils.Constants.IcebergSerializationPolicy;
import org.junit.Before;
import org.junit.experimental.categories.Category;

@Category(IcebergIT.class)
public class IcebergColumnNamesIT extends ColumnNamesITBase {
  @Before
  public void before() throws Exception {
    super.setUp(true, "ZSTD", IcebergSerializationPolicy.OPTIMIZED);
  }
}
