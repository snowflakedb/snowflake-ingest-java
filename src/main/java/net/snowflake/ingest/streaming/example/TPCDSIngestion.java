/*
 * Copyright (c) 2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.example;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestClientInternal;
import net.snowflake.ingest.utils.Constants;

/**
 * Example on how to use the Streaming Ingest client APIs.
 *
 * <p>Please read the README.md file for detailed steps
 */
public class TPCDSIngestion {
    private static String[] tables = new String[]{"call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", "customer_address", "customer_demographics", "date_dim", "dbgen_version", "household_demographics",
            "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site"};
    private static final String originalDatabase = "benchmark_db";
    private static final String originalSchema = "tpcds_sf1";

    private static final String expDatabase = "benchmark_db_streaming_ingest";
    private static final String expSchema = "tpcds_sf1";


    private static Connection jdbcConnection;

    public static void main(String[] args) throws Exception {
        Properties props = Util.getProperties(Constants.BdecVersion.THREE);


        int cores = Runtime.getRuntime().availableProcessors();

        // Create a streaming ingest client
        SnowflakeStreamingIngestClientInternal client =
                (SnowflakeStreamingIngestClientInternal<?>)
                        SnowflakeStreamingIngestClientFactory.builder("gdoci")
                                .setProperties(props)
                                .build();


            int numTables = tables.length; // 1 channel per table

            ExecutorService ingestionThreadPool = Executors.newFixedThreadPool(cores);
            CompletableFuture[] ingestionFutures = new CompletableFuture[numTables];
            List<SnowflakeStreamingIngestChannel> channelList = new ArrayList<>();
            for (int i = 0; i < numTables; i++) {
                int finalI = i;
                final String channelName = "CHANNEL" + i;
                final String table = tables[finalI];
                ingestionFutures[i] =
                        CompletableFuture.runAsync(
                                () -> {
                                    SnowflakeStreamingIngestChannel channel = Util.openChannel(expDatabase, expSchema, table, channelName, client);
                                    channelList.add(channel);
                                        List<Map<String, Object>> rows;
                                    Connection jdbcConnection;
                                    ResultSet result;
                                        // query table
                                    try {
                                        jdbcConnection = Util.getConnection();
                                        jdbcConnection
                                                .createStatement()
                                                .execute(String.format("use warehouse %s", Util.getWarehouse()));

                                        result = jdbcConnection.createStatement().executeQuery(
                                                String.format(
                                                        "select * from %s.%s.%s",
                                                        originalDatabase, originalSchema, table));
                                        rows = Util.resultSetToArrayList(result);
                                        // System.out.println("rows=" + rows);
                                        Util.verifyInsertValidationResponse(channel.insertRows(rows, ""));
                                        Util.waitChannelFlushed(channel, rows.size());
                                        channel.close();
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                },
                                ingestionThreadPool);
            }
            CompletableFuture joined = CompletableFuture.allOf(ingestionFutures);
            joined.get();
            ingestionThreadPool.shutdown();
            client.close();
    }


}
