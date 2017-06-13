package com.datastax.driver.mapping;

public interface ResolvingStrategy
{
    // TODO@i8 clarify whether it's okay that ResolvingStrategy does more than just table/keyspace names
    <T> EntityMapper<T> resolveTable(Class<T> entityClass, MappingManager mappingManager);
}
