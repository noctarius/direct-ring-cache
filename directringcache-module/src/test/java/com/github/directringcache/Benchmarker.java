package com.github.directringcache;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.Clock;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkHistoryChart;
import com.carrotsearch.junitbenchmarks.annotation.LabelType;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSliceSelector;

@RunWith( Parameterized.class )
public class Benchmarker
    extends AbstractBenchmark
{

    @Parameters( name = "Execution {index} - {0}, {1}" )
    public static Collection<Object[]> parameters()
    {
        return TestCaseConstants.EXECUTION_PARAMETER_MUTATIONS;
    }

    private final Random random = new Random( -System.nanoTime() );

    private final PartitionSliceSelector partitionSliceSelector;

    private final PartitionBufferBuilder builder;

    private final PartitionBufferPool pool;

    public Benchmarker( String name1, String name2, PartitionFactory partitionFactory,
                        Class<PartitionSliceSelector> partitionSliceSelectorClass )
        throws InstantiationException, IllegalAccessException
    {
        this.partitionSliceSelector = partitionSliceSelectorClass.newInstance();

        this.builder = new PartitionBufferBuilder( partitionFactory, partitionSliceSelector );
        this.pool = builder.allocatePool( "2M", Runtime.getRuntime().availableProcessors() * 8, "8K" );
    }

    @Test
    @BenchmarkHistoryChart( labelWith = LabelType.RUN_ID, maxRuns = 20 )
    @BenchmarkOptions( warmupRounds = 1000, benchmarkRounds = 20000, clock = Clock.NANO_TIME, concurrency = 20 )
    public void benchmark()
        throws Exception
    {
        PartitionBuffer partitionBuffer = pool.getPartitionBuffer();

        try
        {
            byte[] block = new byte[10 * ( 10 + random.nextInt( 1024 ) )];
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

    @Override
    protected void finalize()
    {
        pool.close();
    }

    private byte[] fill( byte[] block )
    {
        for ( int i = 0; i < block.length; i++ )
        {
            block[i] = (byte) ( 120 + random.nextInt( 100 ) );
        }
        return block;
    }

}
