package com.github.directringcache.impl;

import java.util.concurrent.atomic.AtomicBoolean;

import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionSlice;
import com.github.directringcache.spi.PartitionSliceSelector;

public abstract class AbstractPartition
    implements Partition
{

    protected final PartitionSliceSelector partitionSliceSelector;

    protected final int partitionIndex;

    protected final int sliceByteSize;

    protected final FixedLengthBitSet usedSlices;

    private final boolean pooled;

    private final AtomicBoolean closed = new AtomicBoolean( false );

    protected AbstractPartition( int partitionIndex, int slices, int sliceByteSize,
                                 PartitionSliceSelector partitionSliceSelector, boolean pooled )
    {
        this.partitionIndex = partitionIndex;
        this.sliceByteSize = sliceByteSize;
        this.usedSlices = new FixedLengthBitSet( slices );
        this.partitionSliceSelector = partitionSliceSelector;
        this.pooled = pooled;
    }

    @Override
    public int available()
    {
        return usedSlices.size() - usedSlices.cardinality();
    }

    @Override
    public int used()
    {
        return usedSlices.cardinality();
    }

    @Override
    public int getSliceCount()
    {
        return usedSlices.size();
    }

    @Override
    public int getSliceByteSize()
    {
        return sliceByteSize;
    }

    @Override
    public PartitionSlice get()
    {
        int retry = 0;
        while ( retry++ < 5 )
        {
            int possibleMatch = nextSlice();
            if ( possibleMatch == -1 )
            {
                return null;
            }

            synchronized ( usedSlices )
            {
                if ( !usedSlices.get( possibleMatch ) )
                {
                    if ( usedSlices.testAndSet( possibleMatch ) )
                    {
                        return get( possibleMatch ).lock();
                    }
                }
            }
        }
        return null;
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
        synchronized ( usedSlices )
        {
            slice.clear();
            partitionSliceSelector.freePartitionSlice( this, partitionIndex, partitionSlice.unlock() );
            usedSlices.clear( partitionSlice.index );
        }
    }

    @Override
    public void close()
    {
        if ( !closed.compareAndSet( false, true ) )
        {
            return;
        }

        synchronized ( usedSlices )
        {
            for ( int i = 0; i < getSliceCount(); i++ )
            {
                AbstractPartitionSlice partitionSlice = (AbstractPartitionSlice) get( i );
                partitionSliceSelector.freePartitionSlice( this, partitionIndex, partitionSlice );
                partitionSlice.free();
            }
        }
    }

    @Override
    public boolean isPooled()
    {
        return pooled;
    }

    @Override
    public boolean isClosed()
    {
        return closed.get();
    }

    protected int nextSlice()
    {
        if ( usedSlices.isEmpty() )
        {
            return -1;
        }

        return usedSlices.firstNotSet();
    }

    protected abstract AbstractPartitionSlice get( int index );

}
