package com.github.directringcache.spi;

public interface PartitionFactory
{

    Partition newPartition( int partitionIndex, int sliceByteSize, int slices );

}
