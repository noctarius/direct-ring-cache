package com.github.directringcache.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.directringcache.spi.Partition;

@SuppressWarnings( "restriction" )
class UnsafePartitionSlice
    extends AbstractPartitionSlice
{

    private static final Logger LOGGER = LoggerFactory.getLogger( UnsafePartitionSlice.class );

    private final sun.misc.Unsafe unsafe = BufferUtils.getUnsafe();

    private final Partition partition;

    private final int sliceByteSize;

    final long memoryPointer;

    final long lastMemoryPointer;

    private volatile int writerIndex;

    private volatile int readerIndex;

    UnsafePartitionSlice( int index, Partition partition, int sliceByteSize )
    {
        super( index );

        this.partition = partition;
        this.sliceByteSize = sliceByteSize;
        this.memoryPointer = unsafe.allocateMemory( sliceByteSize );
        this.lastMemoryPointer = memoryPointer + sliceByteSize - 1;
        clear();
    }

    @Override
    public void clear()
    {
        unsafe.setMemory( memoryPointer, sliceByteSize, (byte) 0 );
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
        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "writeToOffset=" + ( memoryPointer + position ) );
        }
        unsafe.putByte( memoryPointer + position, value );
    }

    @Override
    public void put( byte[] array, int offset, int length )
    {
        if ( writerAddress() + length - 1 > lastMemoryPointer )
        {
            throw new IndexOutOfBoundsException( "Writing the array exhausted the available size" );
        }

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "partition=" + partition.getPartitionIndex() + ", sliceIndex=" + index
                + ", writerIndex=" + writerIndex + ", offset=" + offset + ", arrayLength=" + array.length
                + ", writeLength=" + length + ", writerAddress=" + ( memoryPointer + writerIndex ) );
        }
        long memOffset = memoryPointer + readerIndex;
        unsafe.copyMemory( array, BufferUtils.BYTE_ARRAY_OFFSET + offset, null, memOffset, length );
        writerIndex += length;
    }

    @Override
    public byte read()
    {
        return read( readerIndex++ );
    }

    @Override
    public byte read( int position )
    {
        return unsafe.getByte( memoryPointer + position );
    }

    @Override
    public void read( byte[] array, int offset, int length )
    {
        if ( readerAddress() + length - 1 > lastMemoryPointer )
        {
            throw new IndexOutOfBoundsException( "Reading the array exhausted the available size" );
        }

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "partition=" + partition.getPartitionIndex() + ", sliceIndex=" + index
                + ", readerIndex=" + readerIndex + ", offset=" + offset + ", arrayLength=" + array.length
                + ", readLength=" + length + ", readerAddress=" + ( memoryPointer + readerIndex ) );
        }
        long memOffset = memoryPointer + readerIndex;
        unsafe.copyMemory( null, memOffset, array, BufferUtils.BYTE_ARRAY_OFFSET + offset, length );
        readerIndex += length;
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

    private long readerAddress()
    {
        return memoryPointer + readerIndex;
    }

    private long writerAddress()
    {
        return memoryPointer + writerIndex;
    }

    @Override
    protected void free()
    {
        unsafe.setMemory( memoryPointer, sliceByteSize, (byte) 0 );
        unsafe.freeMemory( memoryPointer );
    }
}