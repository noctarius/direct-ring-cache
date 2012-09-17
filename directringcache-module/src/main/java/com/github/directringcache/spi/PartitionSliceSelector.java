package com.github.directringcache.spi;

public interface PartitionSliceSelector {

	PartitionSlice selectPartitionSlice(Partition[] partitions);

}
