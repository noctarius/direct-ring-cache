package com.github.directringcache;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.github.directringcache.impl.BufferUtils;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSliceSelector;

@RunWith( Parameterized.class )
public class PartitionBufferTestCase
{

    @Parameters( name = "Execution {index} - {0}, {1}" )
    public static Collection<Object[]> parameters()
    {
        return TestCaseConstants.EXECUTION_PARAMETER_MUTATIONS;
    }

    private final PartitionFactory partitionFactory;

    private final PartitionSliceSelector partitionSliceSelector;

    public PartitionBufferTestCase( String name1, String name2, PartitionFactory partitionFactory,
                                    Class<PartitionSliceSelector> partitionSliceSelectorClass )
        throws InstantiationException, IllegalAccessException
    {
        this.partitionFactory = partitionFactory;
        this.partitionSliceSelector = partitionSliceSelectorClass.newInstance();
    }

    @Test
    public void testAllocation()
        throws Exception
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "500M", 50, "512K" );

        try
        {
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
            assertEquals( BufferUtils.descriptorToByteSize( "512k" ), partitionBuffer.maxCapacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < 1024 * 1024 + 1; i++ )
            {
                partitionBuffer.writeByte( 1 );
            }
            assertEquals( BufferUtils.descriptorToByteSize( "512k" ) * 3, partitionBuffer.maxCapacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "512k" ) * 2 + 1, partitionBuffer.capacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            pool.freePartitionBuffer( partitionBuffer );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

        }
        finally
        {
            pool.close();
        }
    }

    @Test
    public void testAllocation2()
        throws Exception
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "500M", 50, "256K" );

        try
        {
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes + 1; i++ )
            {
                partitionBuffer.writeByte( 1 );
            }
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
            assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 1, partitionBuffer.capacity() );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

            for ( int i = 0; i < bytes + 1; i++ )
            {
                assertEquals( "Wrong value at position " + i, 1, partitionBuffer.readByte() );
            }

            pool.freePartitionBuffer( partitionBuffer );
            System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                + " bytes), unused " + pool.getFreeSliceCount() );

        }
        finally
        {
            pool.close();
        }
    }

    @Test( expected = RuntimeException.class )
    public void testAllocationBufferFull()
        throws Exception
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "1M", 1, "256K" );

        try
        {
            for ( int o = 0; o < 100; o++ )
            {
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );

                long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

                PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );

                for ( int i = 0; i < bytes + 1; i++ )
                {
                    partitionBuffer.writeByte( 1 );
                }
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 1, partitionBuffer.capacity() );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );

                for ( int i = 0; i < bytes + 1; i++ )
                {
                    assertEquals( "Wrong value at position " + i, 1, partitionBuffer.readByte() );
                }

                pool.freePartitionBuffer( partitionBuffer );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );
            }
        }
        finally
        {
            pool.close();
        }
    }

    @Test
    public void testAllocationFullRound()
        throws Exception
    {
        PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        PartitionBufferPool pool = builder.allocatePool( "10M", 5, "256K" );

        try
        {
            for ( int o = 0; o < 10; o++ )
            {
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );

                long bytes = BufferUtils.descriptorToByteSize( "256K" ) * 20;

                PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ), partitionBuffer.maxCapacity() );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );

                for ( int i = 0; i < bytes + 1; i++ )
                {
                    partitionBuffer.writeByte( 1 );
                }
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 21, partitionBuffer.maxCapacity() );
                assertEquals( BufferUtils.descriptorToByteSize( "256K" ) * 20 + 1, partitionBuffer.capacity() );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );

                for ( int i = 0; i < bytes + 1; i++ )
                {
                    assertEquals( "Wrong value at position " + i, 1, partitionBuffer.readByte() );
                }

                pool.freePartitionBuffer( partitionBuffer );
                System.out.println( "Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory()
                    + " bytes), unused " + pool.getFreeSliceCount() );
            }
        }
        finally
        {
            pool.close();
        }
    }

}
