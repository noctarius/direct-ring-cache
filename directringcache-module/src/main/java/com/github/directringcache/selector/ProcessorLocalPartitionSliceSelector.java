package com.github.directringcache.selector;

import java.util.Arrays;
import java.util.List;

import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionSlice;
import com.github.directringcache.spi.PartitionSliceSelector;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Structure;

public class ProcessorLocalPartitionSliceSelector
    implements PartitionSliceSelector
{

    private final CpuAdapter cpuAdapter = getCpuAdapterSPI();

    private final Partition[] cpuLocalPartition = new Partition[cpuAdapter.getProcessorCount()];

    private volatile int[] assigned;

    @Override
    public PartitionSlice selectPartitionSlice( Partition[] partitions )
    {
        int processorId = cpuAdapter.getCurrentProcessorId();
        Partition partition = cpuLocalPartition[processorId];
        if ( partition != null && partition.available() > 0 )
        {
            return partition.get();
        }

        synchronized ( this )
        {
            if ( assigned == null )
            {
                assigned = new int[partitions.length];
                Arrays.fill( assigned, -1 );
            }

            for ( int index = 0; index < assigned.length; index++ )
            {
                if ( assigned[index] == -1 )
                {
                    assigned[index] = processorId;
                    partition = partitions[index];
                    cpuLocalPartition[processorId] = partition;
                    if ( partition.available() > 0 )
                    {
                        PartitionSlice slice = partition.get();
                        if ( slice != null )
                        {
                            return slice;
                        }
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
            assigned[partitionIndex] = -1;
        }
    }

    private CpuAdapter getCpuAdapterSPI()
    {
        String osName = System.getProperty( "os.name" );
        String osArch = System.getProperty( "os.arch" );
        String osVersion = System.getProperty( "os.version" );

        if ( Platform.isLinux() )
        {
            return new LinuxCpuAdapter();
        }
        else if ( Platform.isWindows() )
        {
            if ( osVersion != null && osVersion.startsWith( "6." ) )
            {
                return new WindowsCpuAdapter();
            }
        }

        throw new UnsupportedOperationSystemException( "OS " + osName + " (" + osVersion + " / " + osArch
            + ") is unsupported for use of cpu local allocation strategy" );
    }

    private static interface CpuAdapter
    {

        int getProcessorCount();

        int getCurrentProcessorId();

    }

    private static class LinuxCpuAdapter
        implements CpuAdapter
    {

        public native int sched_getcpu();

        static
        {
            Native.register( Platform.C_LIBRARY_NAME );
        }

        @Override
        public int getProcessorCount()
        {
            return Runtime.getRuntime().availableProcessors();
        }

        @Override
        public int getCurrentProcessorId()
        {
            return sched_getcpu();
        }
    }

    private static class WindowsCpuAdapter
        implements CpuAdapter
    {

        public native int GetCurrentProcessorNumber();

        public native void GetCurrentProcessorNumberEx( PROCESSOR_NUMBER processorNumber );

        static
        {
            Native.register( "kernel32" );
        }

        private final boolean isLowCpuSystem;

        private WindowsCpuAdapter()
        {
            isLowCpuSystem = getProcessorCount() <= 64;
        }

        @Override
        public int getProcessorCount()
        {
            return Runtime.getRuntime().availableProcessors();
        }

        @Override
        public int getCurrentProcessorId()
        {
            if ( isLowCpuSystem )
                return GetCurrentProcessorNumber();

            PROCESSOR_NUMBER processorNumber = new PROCESSOR_NUMBER();
            GetCurrentProcessorNumberEx( processorNumber );

            return processorNumber.Group * processorNumber.Number;
        }
    }

    public static class PROCESSOR_NUMBER
        extends Structure
    {
        public short Group;

        public byte Number;

        public byte Reserved;

        protected List<?> getFieldOrder()
        {
            return Arrays.asList( "Group", "Number", "Reserved" );
        }
    }

}
