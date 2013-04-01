package com.github.directringcache.impl;

import java.nio.ByteBuffer;

import com.github.directringcache.spi.Partition;

class ByteBufferPartitionSlice
    extends AbstractPartitionSlice
{

    private final ByteBuffer byteBuffer;

    private final Partition partition;

    private final int sliceByteSize;

    private volatile int writerIndex;

    private volatile int readerIndex;

    ByteBufferPartitionSlice( ByteBuffer byteBuffer, int index, Partition partition, int sliceByteSize )
    {
        super( index );

        this.byteBuffer = byteBuffer;
        this.partition = partition;
        this.sliceByteSize = sliceByteSize;
    }

    @Override
    public void clear()
    {
        byteBuffer.clear();
        writerIndex = 0;
        readerIndex = 0;
    }

    @Override
    public void put( byte value )
    {
        put( writerIndex++, value );
    }

    @Override
    public void put( int position, byte value )
    {
        byteBuffer.put( position, value );
    }

    @Override
    public void put( byte[] array, int offset, int length )
    {
        for ( int i = 0; i < length; i++ )
        {
            put( writerIndex++, array[offset + i] );
        }
    }

    @Override
    public byte read()
    {
        return read( readerIndex++ );
    }

    @Override
    public byte read( int position )
    {
        return byteBuffer.get( position );
    }

    @Override
    public void read( byte[] array, int offset, int length )
    {
        for ( int i = 0; i < length; i++ )
        {
            array[offset + i] = read( readerIndex++ );
        }
    }

    @Override
    public int getSliceByteSize()
    {
        return sliceByteSize;
    }

    @Override
    public int readableBytes()
    {
        return writerIndex - readerIndex;
    }

    @Override
    public int writeableBytes()
    {
        return sliceByteSize - writerIndex;
    }

    @Override
    public int writerIndex()
    {
        return writerIndex;
    }

    @Override
    public int readerIndex()
    {
        return readerIndex;
    }

    @Override
    public void writerIndex( int writerIndex )
    {
        BufferUtils.rangeCheck( writerIndex, 0, sliceByteSize, "writerIndex" );
        this.writerIndex = writerIndex;
    }

    @Override
    public void readerIndex( int readerIndex )
    {
        BufferUtils.rangeCheck( readerIndex, 0, sliceByteSize, "readerIndex" );
        this.readerIndex = readerIndex;
    }

    @Override
    public Partition getPartition()
    {
        return partition;
    }

    @Override
    protected void free()
    {
        BufferUtils.cleanByteBuffer( byteBuffer );
    }
}
