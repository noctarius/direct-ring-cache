package com.github.directringcache.spi;

public interface Partition
{

    int available();

    int used();

    int getSliceCount();

    int getSliceByteSize();

    int getPartitionIndex();

    PartitionSlice get();

    void free( PartitionSlice slice );

    void close();

    boolean isPooled();

    boolean isClosed();

}
