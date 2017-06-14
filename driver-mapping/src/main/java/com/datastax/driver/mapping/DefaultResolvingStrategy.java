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

import com.datastax.driver.core.Metadata;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.UDT;
import com.google.common.base.Strings;

/**
 * TODO@i8 document this
 */
public class DefaultResolvingStrategy implements ResolvingStrategy
{
    public ObjectName resolveTable(Class<?> entityClass, MappingManager mappingManager) {
        // TODO@i8 implement naming strategy support (via field)

        Table table = AnnotationChecks.getTypeAnnotation(Table.class, entityClass);

        String ksName = getKeyspace(table);
        if (Strings.isNullOrEmpty(ksName)) {
            String loggedKeyspace = mappingManager.getSession().getLoggedKeyspace();
            if (Strings.isNullOrEmpty(loggedKeyspace))
                throw new IllegalArgumentException(String.format(
                        "Error creating mapper for %s, the @Table annotation declares no default keyspace, and the session is not currently logged to any keyspace",
                        entityClass
                ));
            ksName = Metadata.quote(loggedKeyspace);
        }

        String tableName = getTableName(table);

        return new ObjectName(ksName, tableName);
    }

    protected String getKeyspace(Table table) {
        String result = null;
        String annotationValue = table.keyspace();
        if (annotationValue != null) {
            result = table.caseSensitiveKeyspace() ? Metadata.quote(annotationValue) : annotationValue.toLowerCase();
        }
        return result;
    }

    protected String getTableName(Table table) {
        String result = null;
        String annotationValue = table.name();
        if (annotationValue != null) {
            result = table.caseSensitiveTable() ? Metadata.quote(annotationValue) : annotationValue.toLowerCase();
        }
        return result;
    }

    @Override
    public ObjectName resolveUDT(Class<?> udtClass, MappingManager mappingManager) {
        UDT udt = AnnotationChecks.getTypeAnnotation(UDT.class, udtClass);

        String ksName = getKeyspace(udt);
        if (Strings.isNullOrEmpty(udt.keyspace())) {
            String loggedKeyspace = mappingManager.getSession().getLoggedKeyspace();
            if (Strings.isNullOrEmpty(loggedKeyspace))
                throw new IllegalArgumentException(String.format(
                        "Error creating UDT codec for %s, the @UDT annotation declares no default keyspace, and the session is not currently logged to any keyspace",
                        udtClass
                ));
            ksName = Metadata.quote(loggedKeyspace);
        }

        String udtName = getUDTName(udt);

        return new ObjectName(ksName, udtName);
    }

    protected String getKeyspace(UDT udt) {
        String result = null;
        String annotationValue = udt.keyspace();
        if (annotationValue != null) {
            result = udt.caseSensitiveKeyspace() ? Metadata.quote(annotationValue) : annotationValue.toLowerCase();
        }
        return result;
    }

    protected String getUDTName(UDT udt) {
        String result = null;
        String annotationValue = udt.name();
        if (annotationValue != null) {
            result = udt.caseSensitiveType() ? Metadata.quote(annotationValue) : annotationValue.toLowerCase();
        }
        return result;
    }
}
