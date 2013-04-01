package com.github.directringcache.impl;

import java.nio.ByteBuffer;

import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSliceSelector;

public class ByteBufferPooledPartition
    extends AbstractPooledPartition
{

    public static final PartitionFactory DIRECT_BYTEBUFFER_PARTITION_FACTORY = new PartitionFactory()
    {

        @Override
        public Partition newPartition( int partitionIndex, int sliceByteSize, int slices,
                                       PartitionSliceSelector partitionSliceSelector )
        {
            return new ByteBufferPooledPartition( partitionIndex, slices, sliceByteSize, true, partitionSliceSelector );
        }
    };

    public static final PartitionFactory HEAP_BYTEBUFFER_PARTITION_FACTORY = new PartitionFactory()
    {

        @Override
        public Partition newPartition( int partitionIndex, int sliceByteSize, int slices,
                                       PartitionSliceSelector partitionSliceSelector )
        {
            return new ByteBufferPooledPartition( partitionIndex, slices, sliceByteSize, false, partitionSliceSelector );
        }
    };

    private final ByteBufferPartitionSlice[] slices;

    private ByteBufferPooledPartition( int partitionIndex, int slices, int sliceByteSize, boolean directMemory,
                                       PartitionSliceSelector partitionSliceSelector )
    {
        super( partitionIndex, slices, sliceByteSize, partitionSliceSelector, true );

        this.slices = new ByteBufferPartitionSlice[slices];

        for ( int i = 0; i < slices; i++ )
        {
            ByteBuffer buffer =
                directMemory ? ByteBuffer.allocateDirect( sliceByteSize * slices )
                                : ByteBuffer.allocate( sliceByteSize );
            this.slices[i] = new ByteBufferPartitionSlice( buffer, i, this, sliceByteSize );
        }
    }

    @Override
    protected AbstractPartitionSlice get( int index )
    {
        return slices[index];
    }

}
