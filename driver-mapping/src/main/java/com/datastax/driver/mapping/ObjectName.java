/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.mapping;

import com.google.common.base.Strings;

/**
 * Represents the name of an object in a keyspace, e.g. a table or a user defined type.
 */
public final class ObjectName {
    private final String keyspace;
    private final String name;

    /**
     * Creates a new object name.
     * <p>
     * Note that case-sensitive identifiers should be quoted with {@link com.datastax.driver.core.Metadata#quote} before
     * creating an object name.
     *
     * @param keyspace the name of the keyspace. empty or null refer to the keyspace the session is logged to.
     * @param name     the name of the object, must be a non-empty string
     * @throws java.lang.IllegalArgumentException if name is null or empty
     */
    public ObjectName(String keyspace, String name) {
        this.keyspace = keyspace;

        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("Need non-null non-empty name");
        }
        this.name = name;
    }

    /**
     * Gets the keyspace name
     * @return the keyspace name
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * Gets the object name
     * @return the object name, or null or empty string to use the keyspace the session is logged to.
     */
    public String getName() {
        return name;
    }
}
