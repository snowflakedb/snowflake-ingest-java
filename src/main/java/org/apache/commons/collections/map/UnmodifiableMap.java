/*
 * Copyright (c) 2022-2024 Snowflake Computing Inc. All rights reserved.
 */
package org.apache.commons.collections.map;

import java.util.Collections;
import java.util.Map;

/**
 * Minimum viable implementation for Hadoop Configuration to avoid commons-collections dependency
 */
public class UnmodifiableMap {

    public static Map<?, ?> decorate(final Map<?, ?> map) {
        return Collections.unmodifiableMap(map);
    }
}
