package com.datastax.driver.mapping;

public interface ResolvingStrategy
{
    <T> EntityMapper<T> resolveTable(Class<T> entityClass, MappingManager mappingManager, NamingStrategy namingStrategy);
}
