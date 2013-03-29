package com.github.directringcache.impl;

import java.nio.ByteOrder;
import java.util.Arrays;

import com.github.directringcache.PartitionBuffer;
import com.github.directringcache.PartitionBufferPool;
import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSlice;
import com.github.directringcache.spi.PartitionSliceSelector;

public class PartitionBufferPoolImpl
    implements PartitionBufferPool
{

    private final PartitionSliceSelector partitionSliceSelector;

    private final Partition[] partitions;

    private final int sliceByteSize;

    private final int slices;

    public PartitionBufferPoolImpl( int partitions, int sliceByteSize, int slices, PartitionFactory partitionFactory,
                                    PartitionSliceSelector partitionSliceSelector )
    {

        this.partitions = new Partition[partitions];
        this.partitionSliceSelector = partitionSliceSelector;
        this.sliceByteSize = sliceByteSize;
        this.slices = slices;

        // Initialize partitions
        for ( int i = 0; i < partitions; i++ )
        {
            this.partitions[i] = partitionFactory.newPartition( i, sliceByteSize, slices );
        }
    }

    PartitionSlice requestSlice()
    {
        Partition[] partitionsCopy = Arrays.copyOf( partitions, partitions.length );
        return partitionSliceSelector.selectPartitionSlice( partitionsCopy );
    }

    void freeSlice( PartitionSlice slice )
    {
        if ( slice != null )
        {
            slice.getPartition().free( slice, partitionSliceSelector );
        }
    }

    @Override
    public PartitionBuffer getPartitionBuffer()
    {
        return new PartitionBufferImpl( this, ByteOrder.BIG_ENDIAN );
    }

    @Override
    public void freePartitionBuffer( PartitionBuffer partitionBuffer )
    {
        partitionBuffer.free();
    }

    @Override
    public long getAllocatedMemory()
    {
        return getPartitionCount() * getPartitionByteSize();
    }

    @Override
    public int getPartitionByteSize()
    {
        return getSliceCountPerPartition() * getSliceByteSize();
    }

    @Override
    public int getPartitionCount()
    {
        return partitions.length;
    }

    @Override
    public int getSliceCountPerPartition()
    {
        return slices;
    }

    @Override
    public int getSliceCount()
    {
        return getSliceCountPerPartition() * getPartitionCount();
    }

    @Override
    public int getSliceByteSize()
    {
        return sliceByteSize;
    }

    @Override
    public int getUsedSliceCount()
    {
        int usedSlices = 0;
        for ( Partition partition : partitions )
        {
            usedSlices += partition.used();
        }
        return usedSlices;
    }

    @Override
    public int getFreeSliceCount()
    {
        int available = 0;
        for ( Partition partition : partitions )
        {
            available += partition.available();
        }
        return available;
    }

    @Override
    public void close()
    {
        for ( Partition partition : partitions )
        {
            partition.close();
        }
    }

}
