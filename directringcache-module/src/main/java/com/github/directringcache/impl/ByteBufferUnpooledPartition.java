package com.github.directringcache.impl;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSlice;
import com.github.directringcache.spi.PartitionSliceSelector;

public class ByteBufferUnpooledPartition
    extends AbstractPartition
{

    public static final PartitionFactory DIRECT_BYTEBUFFER_PARTITION_FACTORY = new PartitionFactory()
    {

        @Override
        public Partition newPartition( int partitionIndex, int sliceByteSize, int slices,
                                       PartitionSliceSelector partitionSliceSelector )
        {
            return new ByteBufferUnpooledPartition( partitionIndex, slices, sliceByteSize, true, partitionSliceSelector );
        }
    };

    public static final PartitionFactory HEAP_BYTEBUFFER_PARTITION_FACTORY = new PartitionFactory()
    {

        @Override
        public Partition newPartition( int partitionIndex, int sliceByteSize, int slices,
                                       PartitionSliceSelector partitionSliceSelector )
        {
            return new ByteBufferUnpooledPartition( partitionIndex, slices, sliceByteSize, false,
                                                    partitionSliceSelector );
        }
    };

    private final AtomicInteger index = new AtomicInteger( 0 );

    private final Map<Integer, ByteBufferPartitionSlice> bufferPartitionSlices =
        new ConcurrentHashMap<Integer, ByteBufferPartitionSlice>();

    private final boolean directMemory;

    private ByteBufferUnpooledPartition( int partitionIndex, int slices, int sliceByteSize, boolean directMemory,
                                         PartitionSliceSelector partitionSliceSelector )
    {
        super( partitionIndex, slices, sliceByteSize, partitionSliceSelector, false );

        this.directMemory = directMemory;
    }

    @Override
    public PartitionSlice get()
    {
        ByteBuffer buffer =
            directMemory ? ByteBuffer.allocateDirect( sliceByteSize ) : ByteBuffer.allocate( sliceByteSize );
        ByteBufferPartitionSlice slice = new ByteBufferPartitionSlice( buffer, nextSlice(), this, sliceByteSize );
        bufferPartitionSlices.put( slice.index, slice );
        return slice;
    }

    @Override
    public int available()
    {
        return Integer.MAX_VALUE;
    }

    @Override
    public int used()
    {
        return bufferPartitionSlices.size();
    }

    @Override
    public void free( PartitionSlice slice )
    {
        if ( slice.getPartition() != this )
        {
            throw new IllegalArgumentException( "Given slice cannot be handled by this PartitionBufferPool" );
        }
        if ( !( slice instanceof AbstractPartitionSlice ) )
        {
            throw new IllegalArgumentException( "Given slice cannot be handled by this PartitionBufferPool" );
        }
        AbstractPartitionSlice partitionSlice = (AbstractPartitionSlice) slice;
        bufferPartitionSlices.remove( partitionSlice );
        partitionSlice.free();
    }

    @Override
    public void close()
    {
        if ( !close0() )
        {
            return;
        }

        Iterator<ByteBufferPartitionSlice> iterator = bufferPartitionSlices.values().iterator();
        while ( iterator.hasNext() )
        {
            iterator.next().free();
            iterator.remove();
        }
    }

    @Override
    public int getSliceCount()
    {
        return bufferPartitionSlices.size();
    }

    protected int nextSlice()
    {
        return index.incrementAndGet();
    }

}
