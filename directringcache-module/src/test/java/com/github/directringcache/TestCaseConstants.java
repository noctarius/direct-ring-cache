package com.github.directringcache;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.github.directringcache.impl.ByteBufferPartition;
import com.github.directringcache.impl.UnsafeBufferPartition;
import com.github.directringcache.selector.ProcessorLocalPartitionSliceSelector;
import com.github.directringcache.selector.RoundRobinPartitionSliceSelector;
import com.github.directringcache.selector.ThreadLocalPartitionSliceSelector;

public class TestCaseConstants
{
    public static final Object[] PARTITION_FACTORIES = new Object[] {
        ByteBufferPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY, ByteBufferPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY,
        UnsafeBufferPartition.UNSAFE_PARTITION_FACTORY };

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
                if ( partitionFactory == ByteBufferPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY )
                {
                    partitionFactoryName += "{Direct}";
                }
                else if ( partitionFactory == ByteBufferPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY )
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
