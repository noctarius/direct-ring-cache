package com.github.directringcache;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.github.directringcache.impl.ByteBufferPooledPartition;
import com.github.directringcache.impl.ByteBufferUnpooledPartition;
import com.github.directringcache.impl.UnsafePooledPartition;
import com.github.directringcache.impl.UnsafeUnpooledPartition;
import com.github.directringcache.selector.ProcessorLocalPartitionSliceSelector;
import com.github.directringcache.selector.RoundRobinPartitionSliceSelector;
import com.github.directringcache.selector.ThreadLocalPartitionSliceSelector;

public class TestCaseConstants
{
    public static final Object[] PARTITION_FACTORIES =
        new Object[] { ByteBufferPooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY,
            ByteBufferPooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY,
            UnsafePooledPartition.UNSAFE_PARTITION_FACTORY,
            ByteBufferUnpooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY,
            ByteBufferUnpooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY,
            UnsafeUnpooledPartition.UNSAFE_PARTITION_FACTORY };

    public static final Object[] PARTITION_SLICE_SELECTORS = new Object[] { RoundRobinPartitionSliceSelector.class,
        ThreadLocalPartitionSliceSelector.class, ProcessorLocalPartitionSliceSelector.class };

    public static final Collection<Object[]> EXECUTION_PARAMETER_MUTATIONS = buildExecutionParameterMutations();

    private static List<Object[]> buildExecutionParameterMutations()
    {
        List<Object[]> mutations = new LinkedList<Object[]>();

        for ( Object partitionFactory : PARTITION_FACTORIES )
        {
            for ( Object partitionSliceSelector : PARTITION_SLICE_SELECTORS )
            {
                String partitionFactoryName = partitionFactory.getClass().getEnclosingClass().getSimpleName();
                if ( partitionFactory == ByteBufferPooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY
                    || partitionFactory == ByteBufferUnpooledPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY )
                {
                    partitionFactoryName += "{Direct}";
                }
                else if ( partitionFactory == ByteBufferPooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY
                    || partitionFactory == ByteBufferUnpooledPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY )
                {
                    partitionFactoryName += "{Heap}";
                }
                mutations.add( new Object[] { partitionFactoryName,
                    ( (Class<?>) partitionSliceSelector ).getSimpleName(), partitionFactory, partitionSliceSelector } );
            }
        }

        return mutations;
    }

}
