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

/**
 * TODO@i8 document this
 */
public class DefaultResolvingStrategy implements ResolvingStrategy
{
    public ObjectName resolveTable(Class<?> entityClass, MappingManager mappingManager) {
        // TODO@i8 implement naming strategy support (via field)
        // look at com.datastax.driver.mapping.DefaultPropertyMapper.inferMappedName() for an example
        // note that currently, the name of a table is a required field of the annotation
        // choose a default naming strategy for tables/UDTs analogous to the default behavior for property names (where supplying a name in the annotation is optional)

        Table table = AnnotationChecks.getTypeAnnotation(Table.class, entityClass);
        String ksName = getKeyspace(table);
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
