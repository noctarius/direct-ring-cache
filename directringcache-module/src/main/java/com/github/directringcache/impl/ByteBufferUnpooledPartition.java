package com.github.directringcache.impl;

import java.nio.ByteBuffer;

import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSliceSelector;

public class ByteBufferUnpooledPartition
    extends AbstractUnpooledPartition
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

    private final boolean directMemory;

    private ByteBufferUnpooledPartition( int partitionIndex, int slices, int sliceByteSize, boolean directMemory,
                                         PartitionSliceSelector partitionSliceSelector )
    {
        super( partitionIndex, slices, sliceByteSize, partitionSliceSelector, false );

        this.directMemory = directMemory;
    }

    @Override
    protected AbstractPartitionSlice createPartitionSlice( int index, int sliceByteSize )
    {
        ByteBuffer buffer =
            directMemory ? ByteBuffer.allocateDirect( sliceByteSize ) : ByteBuffer.allocate( sliceByteSize );
        return new ByteBufferPartitionSlice( buffer, nextSlice(), this, sliceByteSize );
    }

}
