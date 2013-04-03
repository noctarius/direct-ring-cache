package com.github.directringcache;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSliceSelector;
import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;

@SuppressWarnings( { "unchecked", "rawtypes" } )
public class CaliperBenchmarker
    extends SimpleBenchmark
{

    private static final Map<String, PartitionFactory> PARTITION_FACTORIES = new HashMap<String, PartitionFactory>();

    private static final Map<String, Class<? extends PartitionSliceSelector>> PARTITION_SLICE_SELECTORS =
        new HashMap<String, Class<? extends PartitionSliceSelector>>();

    static
    {
        for ( Object partitionFactory : TestCaseConstants.PARTITION_FACTORIES )
        {
            PARTITION_FACTORIES.put( TestCaseConstants.buildPartitionFactoryName( partitionFactory ),
                                     (PartitionFactory) partitionFactory );
        }

        for ( Object partitionSliceSelector : TestCaseConstants.PARTITION_SLICE_SELECTORS )
        {
            PARTITION_SLICE_SELECTORS.put( ( (Class<?>) partitionSliceSelector ).getName(),
                                           (Class) partitionSliceSelector );
        }
    }

    public static void main( String[] args )
    {
        Runner.main( CaliperBenchmarker.class, args );
    }

    @Param
    private String partitionSliceSelectorName;

    @Param
    private String partitionFactoryName;

    @Param( value = { "1", "5", "10", "20", "25" } )
    private int concurrency;

    public void time( final int rounds )
        throws Exception
    {
        final PartitionFactory partitionFactory = PARTITION_FACTORIES.get( partitionFactoryName );
        final PartitionSliceSelector partitionSliceSelector =
            PARTITION_SLICE_SELECTORS.get( partitionSliceSelectorName ).newInstance();

        final PartitionBufferBuilder builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        final PartitionBufferPool pool =
            builder.allocatePool( "2M", Runtime.getRuntime().availableProcessors() * 8, "8K" );

        try
        {
            final CountDownLatch latch = new CountDownLatch( concurrency );
            final ExecutorService executorService = Executors.newFixedThreadPool( concurrency );

            executorService.execute( new Runnable()
            {
                private final Random random = new Random( -System.nanoTime() );

                @Override
                public void run()
                {
                    try
                    {
                        for ( int round = 0; round < rounds; round++ )
                        {
                            PartitionBuffer partitionBuffer = pool.getPartitionBuffer();

                            try
                            {
                                byte[] block = new byte[10 * 2096];
                                partitionBuffer.writeBytes( fill( block ) );

                                byte[] result = new byte[block.length];
                                partitionBuffer.readBytes( result );

                                for ( int i = 0; i < block.length; i++ )
                                {
                                    if ( block[i] != result[i] )
                                    {
                                        System.out.println( Arrays.toString( block ) );
                                        System.out.println( Arrays.toString( result ) );
                                        throw new Exception( "Arrays don't match at index=" + i );
                                    }
                                }
                            }
                            finally
                            {
                                partitionBuffer.free();
                            }
                        }

                        latch.countDown();
                    }
                    catch ( Throwable t )
                    {
                        t.printStackTrace();
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

            latch.await();
        }
        finally
        {
            pool.close();
        }

        System.out.println( "Running finalization..." );
        System.runFinalization();
        System.gc();
    }

    public static Collection<String> partitionFactoryNameValues()
    {
        return PARTITION_FACTORIES.keySet();
    }

    public static Collection<String> partitionSliceSelectorNameValues()
    {
        return PARTITION_SLICE_SELECTORS.keySet();
    }

}
