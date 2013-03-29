package com.github.directringcache.selector;

import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionSlice;
import com.github.directringcache.spi.PartitionSliceSelector;

public class ThreadLocalPartitionSliceSelector
    implements PartitionSliceSelector
{

    private final ThreadLocal<Partition> partitionAssignment = new ThreadLocal<Partition>();

    private volatile boolean[] assigned;

    @Override
    public PartitionSlice selectPartitionSlice( Partition[] partitions )
    {
        Partition partition = partitionAssignment.get();
        if ( partition != null && partition.available() > 0 )
        {
            return partition.get();
        }

        synchronized ( this )
        {
            if ( assigned == null )
            {
                assigned = new boolean[partitions.length];
            }

            for ( int index = 0; index < assigned.length; index++ )
            {
                if ( !assigned[index] )
                {
                    assigned[index] = true;
                    partition = partitions[index];
                    partitionAssignment.set( partition );
                    PartitionSlice slice = partition.get();
                    if ( slice != null )
                    {
                        return slice;
                    }
                }
            }

            for ( int index = 0; index < partitions.length; index++ )
            {
                if ( partitions[index].available() > 0 )
                {
                    PartitionSlice slice = partitions[index].get();
                    if ( slice != null )
                    {
                        return slice;
                    }
                }
            }
        }

        throw new RuntimeException( "Could not retrieve a new partition slice" );
    }

    @Override
    public void freePartitionSlice( Partition partition, int partitionIndex, PartitionSlice slice )
    {
        if ( partition.available() == 0 )
        {
            assigned[partitionIndex] = false;
        }
    }

}
