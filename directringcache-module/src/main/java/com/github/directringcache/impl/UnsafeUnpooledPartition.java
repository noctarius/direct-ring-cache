package com.github.directringcache.impl;

import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSliceSelector;

public class UnsafeUnpooledPartition
    extends AbstractUnpooledPartition
{

    public static final PartitionFactory UNSAFE_POOLED_PARTITION_FACTORY = new PartitionFactory()
    {

        @Override
        public Partition newPartition( int partitionIndex, int sliceByteSize, int slices,
                                       PartitionSliceSelector partitionSliceSelector )
        {
            return new UnsafeUnpooledPartition( partitionIndex, slices, sliceByteSize, partitionSliceSelector );
        }
    };

    private UnsafeUnpooledPartition( int partitionIndex, int slices, int sliceByteSize,
                                     PartitionSliceSelector partitionSliceSelector )
    {
        super( partitionIndex, slices, sliceByteSize, partitionSliceSelector, true );
    }

    @Override
    protected AbstractPartitionSlice createPartitionSlice( int index, int sliceByteSize )
    {
        return new UnsafePartitionSlice( index, this, sliceByteSize );
    }

}
