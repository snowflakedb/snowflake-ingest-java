package net.snowflake.ingest.streaming.internal;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ConstantParameterProviderTest {
  @Parameterized.Parameters(name = "isIcebergMode: {0}")
  public static Object[] isIcebergMode() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public static boolean isIcebergMode;

  @Test
  public void testConstantParameterProvider() {
    ConstantParameterProvider constantParameterProvider =
        new ConstantParameterProvider(isIcebergMode);
    assert constantParameterProvider.getEnableChunkEncryption() == !isIcebergMode;
  }
}
