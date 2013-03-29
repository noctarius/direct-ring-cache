package com.github.directringcache.spi;

public interface Partition {

	int available();

	int used();

	int getSliceByteSize();

	PartitionSlice get();

	void free(PartitionSlice slice, PartitionSliceSelector partitionSliceSelector);

	void close();

}
