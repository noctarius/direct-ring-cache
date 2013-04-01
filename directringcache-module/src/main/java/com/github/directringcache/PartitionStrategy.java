package com.github.directringcache;

import com.github.directringcache.impl.ByteBufferPooledPartition;
import com.github.directringcache.impl.UnsafePooledPartition;
import com.github.directringcache.spi.PartitionFactory;

public enum PartitionStrategy
{

    POOLED_UNSAFE( UnsafePooledPartition.UNSAFE_POOLED_PARTITION_FACTORY ), //
    POOLED_BYTEBUFFER_HEAP( ByteBufferPooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY ), //
    POOLED_BYTEBUFFER_DIRECT( ByteBufferPooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY ), //
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
