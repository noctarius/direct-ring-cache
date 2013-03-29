package com.github.directringcache.spi;

public interface PartitionSlice
{

    void clear();

    void put( byte value );

    void put( int position, byte value );

    void put( byte[] array, int offset, int length );

    byte read();

    byte read( int position );

    void read( byte[] array, int offset, int length );

    int readableBytes();

    int writeableBytes();

    int writerIndex();

    int readerIndex();

    void writerIndex( int writerIndex );

    void readerIndex( int readerIndex );

    Partition getPartition();

}
