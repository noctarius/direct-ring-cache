package com.github.directringcache.impl;

import java.util.concurrent.atomic.AtomicBoolean;

import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionSliceSelector;

public abstract class AbstractPartition
    implements Partition
{

    protected final PartitionSliceSelector partitionSliceSelector;

    protected final int partitionIndex;

    protected final int sliceByteSize;

    private final AtomicBoolean closed = new AtomicBoolean( false );

    private final boolean pooled;

    protected AbstractPartition( int partitionIndex, int slices, int sliceByteSize,
                                 PartitionSliceSelector partitionSliceSelector, boolean pooled )
    {
        this.partitionIndex = partitionIndex;
        this.sliceByteSize = sliceByteSize;
        this.partitionSliceSelector = partitionSliceSelector;
        this.pooled = pooled;
    }

    @Override
    public int getSliceByteSize()
    {
        return sliceByteSize;
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

    protected boolean close0()
    {
        if ( !closed.compareAndSet( false, true ) )
        {
            return false;
        }
        return true;
    }

}
