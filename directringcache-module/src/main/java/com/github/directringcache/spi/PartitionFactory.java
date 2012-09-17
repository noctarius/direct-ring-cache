package com.github.directringcache.spi;

public interface PartitionFactory {

	Partition newPartition(int sliceByteSize, int slices);

}
