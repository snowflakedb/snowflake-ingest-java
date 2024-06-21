/*
 * Copyright (c) 2024. Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class InternalParameterProviderTest {
  @Parameterized.Parameters(name = "isIcebergMode: {0}")
  public static Object[] isIcebergMode() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public static boolean isIcebergMode;

  @Test
  public void testConstantParameterProvider() {
    InternalParameterProvider internalParameterProvider =
        new InternalParameterProvider(isIcebergMode);
    assert internalParameterProvider.getEnableChunkEncryption() == !isIcebergMode;
  }
}
