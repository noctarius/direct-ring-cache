package com.github.directringcache;

public interface PartitionBufferPool {

	PartitionBuffer getPartitionBuffer();

	void freePartitionBuffer(PartitionBuffer partitionBuffer);

	long getAllocatedMemory();

	int getPartitionByteSize();

	int getPartitionCount();

	int getSliceCountPerPartition();

	int getSliceCount();

	int getSliceByteSize();

	int getUsedSliceCount();

	int getFreeSliceCount();

	void close();

}
