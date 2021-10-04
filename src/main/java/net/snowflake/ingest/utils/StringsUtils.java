/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.ingest.utils;

public class StringsUtils {
    public static boolean isNullOrEmpty(String string){
        return string == null || string.isEmpty();
    }
}
