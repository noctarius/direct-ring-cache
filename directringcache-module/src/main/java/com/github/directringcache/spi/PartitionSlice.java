package com.github.directringcache.spi;

public interface PartitionSlice {

	void clear();

	void put(byte value);

	void put(int position, byte value);

	byte read();

	byte read(int position);

	int readableBytes();

	int writeableBytes();

	int writerIndex();

	int readerIndex();

	void writerIndex(int writerIndex);

	void readerIndex(int readerIndex);

	Partition getPartition();

}
