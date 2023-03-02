package net.snowflake.ingest.streaming.example;

import java.util.Arrays;
import java.util.Collection;

import net.snowflake.ingest.utils.Constants;

public class ParquetPerf {

    private static int numRuns = 1;

    public static Collection<Object[]> getParameters() {
        return Arrays.asList(
                new Object[][]{
                        // 1M X 1
                        {"Arrow", false, Constants.BdecVersion.ONE, 100000, 1, 1, true},
                        {"Parquet", false, Constants.BdecVersion.THREE, 100000, 1, 1, true},
                        // 1M X 2
                        {"Arrow", false, Constants.BdecVersion.ONE, 1000000, 1, 2, true},
                        {"Parquet", false, Constants.BdecVersion.THREE, 1000000, 1, 2, true},
                        // 1M X 3
                        {"Arrow", false, Constants.BdecVersion.ONE, 1000000, 1, 3, true},
                        {"Parquet", false, Constants.BdecVersion.THREE, 1000000, 1, 3, true},
                });
    }

    public static void main(String[] args) {
        Collection<Object[]> param = getParameters();

        boolean[] nullables = {false};
        for (boolean n : nullables) {
            for (Object[] p : param) {
                for (int i = 0; i < numRuns; i++) {
                    Runner perfRunner =
                            new Runner(
                                    (String) p[0],
                                    (Boolean) p[1],
                                    (Constants.BdecVersion) p[2],
                                    (Integer) p[3],
                                    (Integer) p[4],
                                    (Integer) p[5],
                                    n,
                                    (Boolean) p[6]);
                    try {

                        perfRunner.setup();
                        perfRunner.runPerfExperiment();
                        perfRunner.tearDown();
                        System.out.println("Run with " + Arrays.toString(p));

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
}
