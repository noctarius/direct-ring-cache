package com.github.directringcache.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.directringcache.spi.PartitionSlice;
import com.github.directringcache.spi.PartitionSliceSelector;

public abstract class AbstractUnpooledPartition
    extends AbstractPartition
{

    protected AbstractUnpooledPartition( int partitionIndex, int slices, int sliceByteSize,
                                         PartitionSliceSelector partitionSliceSelector, boolean pooled )
    {
        super( partitionIndex, slices, sliceByteSize, partitionSliceSelector, pooled );
    }

    private final AtomicInteger index = new AtomicInteger( 0 );

    private final Map<Integer, AbstractPartitionSlice> bufferPartitionSlices =
        new ConcurrentHashMap<Integer, AbstractPartitionSlice>();

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
    public int getSliceCount()
    {
        return bufferPartitionSlices.size();
    }

    @Override
    public PartitionSlice get()
    {
        AbstractPartitionSlice slice = createPartitionSlice( nextSlice(), sliceByteSize );
        bufferPartitionSlices.put( slice.index, slice );
        return slice;
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

        Iterator<AbstractPartitionSlice> iterator = bufferPartitionSlices.values().iterator();
        while ( iterator.hasNext() )
        {
            iterator.next().free();
            iterator.remove();
        }
    }

    protected int nextSlice()
    {
        return index.incrementAndGet();
    }

    protected abstract AbstractPartitionSlice createPartitionSlice( int index, int sliceByteSize );

}
