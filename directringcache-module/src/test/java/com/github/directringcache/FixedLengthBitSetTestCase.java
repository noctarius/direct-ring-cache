package com.github.directringcache;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.github.directringcache.impl.FixedLengthBitSet;

public class FixedLengthBitSetTestCase
{

    @Test
    public void testRanOutOfBits()
        throws Exception
    {
        FixedLengthBitSet bitSet = new FixedLengthBitSet( 100 );
        for ( int i = 0; i < 100; i++ )
        {
            int index = bitSet.nextNotSet( i );
            bitSet.testAndSet( index );
            assertEquals( index, i );
            assertEquals( bitSet.cardinality(), i + 1 );
        }

        assertEquals( -1, bitSet.nextNotSet( 0 ) );
    }

    @Test
    public void testNextNotSet1()
        throws Exception
    {
        FixedLengthBitSet bitSet = new FixedLengthBitSet( 10 );
        for ( int i = 0; i < 10; i++ )
        {
            int index = bitSet.nextNotSet( i );
            bitSet.testAndSet( index );
            assertEquals( index, i );
            assertEquals( bitSet.cardinality(), i + 1 );
        }

        bitSet.clear( 6 );
        assertEquals( 6, bitSet.nextNotSet( 9 ) );
        assertEquals( 6, bitSet.nextNotSet( 0 ) );
    }

    @Test
    public void testNextNotSet2()
        throws Exception
    {
        FixedLengthBitSet bitSet = new FixedLengthBitSet( 129 );
        for ( int i = 0; i < 129; i++ )
        {
            int index = bitSet.nextNotSet( i );
            bitSet.testAndSet( index );
            assertEquals( index, i );
            assertEquals( bitSet.cardinality(), i + 1 );
        }

        bitSet.clear( 6 );
        assertEquals( 6, bitSet.nextNotSet( 128 ) );
        assertEquals( 6, bitSet.nextNotSet( 0 ) );
    }

}
