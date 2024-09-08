/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.streaming.internal;

interface IStorage {
    void put(String blobPath, byte[] blob);
}
