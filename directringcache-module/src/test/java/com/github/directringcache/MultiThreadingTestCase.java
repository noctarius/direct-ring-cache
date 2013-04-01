package com.github.directringcache;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.github.directringcache.impl.UnsafePooledPartition;
import com.github.directringcache.selector.ProcessorLocalPartitionSliceSelector;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSliceSelector;

public class MultiThreadingTestCase
{

    public static void main( String[] args )
        throws Exception
    {
        final int processorCount = Runtime.getRuntime().availableProcessors();

        final PartitionFactory partitionFactory = UnsafePooledPartition.UNSAFE_PARTITION_FACTORY;
        final PartitionSliceSelector partitionSliceSelector = new ProcessorLocalPartitionSliceSelector();

        final PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        final PartitionBufferPool pool = builder.allocatePool( "1M", processorCount * 8, "8K" );

        final ExecutorService executorService = Executors.newFixedThreadPool( processorCount * 2 + 1 );

        final CountDownLatch latch = new CountDownLatch( processorCount * 5 );

        for ( int i = 0; i < processorCount * 5; i++ )
        {
            final int index = i;
            executorService.execute( new Runnable()
            {

                private final Random random = new Random( -System.nanoTime() );

                private final int maxRuns = index % 2 == 0 ? 10000 : 8000;

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

                        final byte[] block = new byte[10 * ( 1024 + random.nextInt( 1024 ) )];
                        partitionBuffer.writeBytes( fill( block ) );

                        o++;

                        try
                        {
                            Thread.sleep( random.nextInt( 100 ) );
                        }
                        catch ( Exception e )
                        {
                        }

                        byte[] result = new byte[block.length];
                        partitionBuffer.readBytes( result );

                        for ( int i = 0; i < block.length; i++ )
                        {
                            if ( block[i] != result[i] )
                            {
                                System.err.println( "Arrays don't match at index=" + i );
                                break;
                            }
                        }

                        partitionBuffer.free();
                        executorService.execute( this );
                    }
                    catch ( Throwable t )
                    {
                        latch.countDown();
                    }
                }

                private byte[] fill( byte[] block )
                {
                    for ( int i = 0; i < block.length; i++ )
                    {
                        block[i] = (byte) ( 120 + random.nextInt( 100 ) );
                    }
                    return block;
                }
            } );
        }

        latch.await();
        pool.close();
        executorService.shutdown();
    }
}
