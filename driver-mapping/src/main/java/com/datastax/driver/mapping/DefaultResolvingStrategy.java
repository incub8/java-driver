package com.datastax.driver.mapping;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.base.Strings;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultResolvingStrategy implements ResolvingStrategy
{
    private static final Comparator<AliasedMappedProperty> POSITION_COMPARATOR = new Comparator<AliasedMappedProperty>() {
        @Override
        public int compare(AliasedMappedProperty o1, AliasedMappedProperty o2) {
            return o1.mappedProperty.getPosition() - o2.mappedProperty.getPosition();
        }
    };

    public <T> EntityMapper<T> resolveTable(Class<T> entityClass, MappingManager mappingManager) {
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

        ConsistencyLevel writeConsistency = table.writeConsistency().isEmpty() ? null : ConsistencyLevel.valueOf(table.writeConsistency().toUpperCase());
        ConsistencyLevel readConsistency = table.readConsistency().isEmpty() ? null : ConsistencyLevel.valueOf(table.readConsistency().toUpperCase());

        KeyspaceMetadata keyspaceMetadata = mappingManager.getSession().getCluster().getMetadata().getKeyspace(ksName);
        if (keyspaceMetadata == null)
            throw new IllegalArgumentException(String.format("Keyspace %s does not exist", ksName));

        AbstractTableMetadata tableMetadata = keyspaceMetadata.getTable(tableName);
        if (tableMetadata == null) {
            tableMetadata = keyspaceMetadata.getMaterializedView(tableName);
            if (tableMetadata == null)
                throw new IllegalArgumentException(String.format("Table or materialized view %s does not exist in keyspace %s", tableName, ksName));
        }

        EntityMapper<T> mapper = new EntityMapper<T>(entityClass, ksName, tableName, writeConsistency, readConsistency);

        List<AliasedMappedProperty> pks = new ArrayList<AliasedMappedProperty>();
        List<AliasedMappedProperty> ccs = new ArrayList<AliasedMappedProperty>();
        List<AliasedMappedProperty> rgs = new ArrayList<AliasedMappedProperty>();

        MappingConfiguration configuration = mappingManager.getConfiguration();
        Set<? extends MappedProperty<?>> properties = configuration.getPropertyMapper().mapTable(entityClass);
        AtomicInteger columnCounter = mappingManager.protocolVersionAsInt == 1 ? null : new AtomicInteger(0);

        for (MappedProperty<?> mappedProperty : properties) {

            String alias = (columnCounter != null)
                    ? "col" + columnCounter.incrementAndGet()
                    : null;

            AliasedMappedProperty aliasedMappedProperty = new AliasedMappedProperty(mappedProperty, alias);

            if (mappingManager.protocolVersionAsInt == 1 && mappedProperty.isComputed())
                throw new UnsupportedOperationException("Computed properties are not supported with native protocol v1");

            if (!mappedProperty.isComputed() && tableMetadata.getColumn(mappedProperty.getMappedName()) == null)
                throw new IllegalArgumentException(String.format("Column %s does not exist in table %s.%s",
                        mappedProperty.getMappedName(), ksName, tableName));

            if (mappedProperty.isPartitionKey())
                pks.add(aliasedMappedProperty);
            else if (mappedProperty.isClusteringColumn())
                ccs.add(aliasedMappedProperty);
            else
                rgs.add(aliasedMappedProperty);

            // if the property is of a UDT type, parse it now
            for (Class<?> udt : TypeMappings.findUDTs(mappedProperty.getPropertyType().getType()))
                mappingManager.getUDTCodec(udt);
        }

        Collections.sort(pks, POSITION_COMPARATOR);
        Collections.sort(ccs, POSITION_COMPARATOR);

        AnnotationChecks.validateOrder(pks, "@PartitionKey");
        AnnotationChecks.validateOrder(ccs, "@ClusteringColumn");

        mapper.addColumns(pks, ccs, rgs);
        return mapper;
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
}
