package com.github.directringcache;

import com.github.directringcache.impl.ByteBufferPooledPartition;
import com.github.directringcache.impl.ByteBufferUnpooledPartition;
import com.github.directringcache.impl.UnsafePooledPartition;
import com.github.directringcache.impl.UnsafeUnpooledPartition;
import com.github.directringcache.spi.PartitionFactory;

public enum PartitionStrategy
{

    POOLED_UNSAFE( UnsafePooledPartition.UNSAFE_PARTITION_FACTORY ), //
    POOLED_BYTEBUFFER_HEAP( ByteBufferPooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY ), //
    POOLED_BYTEBUFFER_DIRECT( ByteBufferPooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY ), //
    UNPOOLED_UNSAFE( UnsafeUnpooledPartition.UNSAFE_PARTITION_FACTORY ), //
    UNPOOLED_BYTEBUFFER_HEAP( ByteBufferUnpooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY ), //
    UNPOOLED_BYTEBUFFER_DIRECT( ByteBufferUnpooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY ), //
    ;

    private final PartitionFactory partitionFactory;

    private PartitionStrategy( PartitionFactory partitionFactory )
    {
        this.partitionFactory = partitionFactory;
    }

    public PartitionFactory getPartitionFactory()
    {
        return partitionFactory;
    }

}
