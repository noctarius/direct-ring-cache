package com.github.directringcache;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.github.directringcache.impl.UnsafeBufferPartition;
import com.github.directringcache.selector.ProcessorLocalPartitionSliceSelector;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSliceSelector;

public class MultiThreadingTestCase
{

    public static void main( String[] args )
        throws Exception
    {
        final int processorCount = Runtime.getRuntime().availableProcessors();

        final PartitionFactory partitionFactory = UnsafeBufferPartition.UNSAFE_PARTITION_FACTORY;
        final PartitionSliceSelector partitionSliceSelector = new ProcessorLocalPartitionSliceSelector();

        final PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        final PartitionBufferPool pool = builder.allocatePool( "1M", processorCount * 8, "8K" );

        final byte[] block10 = new byte[10 * 1024];
        Arrays.fill( block10, (byte) 0 );

        final byte[] block20 = new byte[20 * 1024];
        Arrays.fill( block20, (byte) 0 );

        final ExecutorService executorService = Executors.newFixedThreadPool( processorCount + 1 );

        final CountDownLatch latch = new CountDownLatch( processorCount * 5 );

        for ( int i = 0; i < processorCount * 5; i++ )
        {
            final int index = i;
            executorService.execute( new Runnable()
            {

                private final Random random = new Random( -System.nanoTime() );

                private final int maxRuns = index % 2 == 0 ? 1000 : 800;

                private volatile int o = 0;

                public void run()
                {
                    try
                    {
                        PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
                        if ( o >= maxRuns )
                        {
                            latch.countDown();
                            return;
                        }

                        if ( random.nextBoolean() )
                        {
                            partitionBuffer.writeBytes( block20 );
                        }
                        else
                        {
                            partitionBuffer.writeBytes( block10 );
                        }

                        o++;

                        try
                        {
                            Thread.sleep( random.nextInt( 100 ) );
                        }
                        catch ( Exception e )
                        {
                        }

                        partitionBuffer.free();
                        executorService.execute( this );
                    }
                    catch ( Throwable t )
                    {
                        latch.countDown();
                    }
                }
            } );
        }

        latch.await();
        pool.close();
        executorService.shutdown();
    }
}
