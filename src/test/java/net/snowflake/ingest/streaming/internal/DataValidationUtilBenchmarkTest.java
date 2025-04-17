/*
 * Copyright (c) 2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

@State(Scope.Thread)
public class DataValidationUtilBenchmarkTest {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final Random random = new Random(0x5EED);

  private static final int numRow = 100000;

  private List<Object> rows;

  @Setup(Level.Trial)
  public void setUp() {
    rows = new ArrayList<>();
    for (int i = 0; i < numRow; i++) {
      Object obj = createRandomObject();
      if (i % 2 == 0) {
        obj = objectMapper.valueToTree(obj).toString();
      }
      rows.add(obj);
    }
  }

  @Benchmark
  public void testValidateAndParseVariantNew() {
    for (int i = 0; i < numRow; i++) {
      DataValidationUtil.validateAndParseVariantNew("COL", rows.get(i), i);
    }
  }

  @Test
  public void launchBenchmark() throws RunnerException {
    Options opt =
        new OptionsBuilder()
            // Specify which benchmarks to run.
            // You can be more specific if you'd like to run only one benchmark per test.
            .include(this.getClass().getName() + ".*")
            // Set the following options as needed
            .mode(Mode.AverageTime)
            .timeUnit(TimeUnit.MICROSECONDS)
            .warmupTime(TimeValue.seconds(1))
            .warmupIterations(2)
            .measurementTime(TimeValue.seconds(1))
            .measurementIterations(10)
            .threads(2)
            .forks(1)
            .shouldFailOnError(true)
            .shouldDoGC(true)
            // .jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintInlining")
            // .addProfiler(WinPerfAsmProfiler.class)
            .build();

    new Runner(opt).run();
  }

  private static final List<List<Integer>> randomOpts =
      Arrays.asList(
          Arrays.asList(1, 200),
          Arrays.asList(2, 14),
          Arrays.asList(3, 5),
          Arrays.asList(4, 3),
          Arrays.asList(5, 2),
          Arrays.asList(6, 2),
          Arrays.asList(7, 2));

  private Map<String, Object> createRandomObject() {
    List<Integer> opts = randomOpts.get(random.nextInt(randomOpts.size()));
    return createObject(random.nextInt(opts.get(0)) + 1, random.nextInt(opts.get(1)) + 1);
  }

  private Map<String, Object> createObject(int depth, int width) {
    if (depth == 1) {
      return new HashMap<String, Object>() {
        {
          put("key", "value");
        }
      };
    } else {
      Map<String, Object> result = createObject(depth - 1, width);
      for (int i = 0; i < width; i++) {
        result.put("key" + i, createObject(depth - 1, width));
      }
      return result;
    }
  }
}
