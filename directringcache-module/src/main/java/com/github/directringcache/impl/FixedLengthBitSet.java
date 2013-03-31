package com.github.directringcache.impl;

import java.util.concurrent.atomic.AtomicLongArray;

public class FixedLengthBitSet
{

    private final int bits;

    private final AtomicLongArray words;

    public FixedLengthBitSet( int bits )
    {
        this.bits = bits;
        this.words = new AtomicLongArray( bits % 64 == 0 ? bits / 64 : bits / 64 + 1 );
    }

    public void flip( int bitIndex )
    {
        BufferUtils.rangeCheck( bitIndex, 0, bits, "bitIndex" );
        int index = index( bitIndex );
        long mask = 1L << bitIndex;
        long value = words.get( index );
        long newValue = value ^ mask;
        while ( !words.compareAndSet( index, value, newValue ) )
        {
            value = words.get( index );
            newValue = value ^ mask;
        }
    }

    public boolean get( int bitIndex )
    {
        BufferUtils.rangeCheck( bitIndex, 0, bits, "bitIndex" );
        int index = index( bitIndex );
        long mask = 1L << bitIndex;
        return ( words.get( index ) & mask ) != 0;
    }

    public boolean testAndSet( int bitIndex )
    {
        BufferUtils.rangeCheck( bitIndex, 0, bits, "bitIndex" );
        int index = index( bitIndex );
        long mask = 1L << bitIndex;
        long value = words.get( index );
        long newValue = value | mask;
        while ( !words.compareAndSet( index, value, newValue ) )
        {
            value = words.get( index );
            if ( ( value & mask ) != 0 )
            {
                return false;
            }
            newValue = value | mask;
        }
        return true;
    }

    public void set( int bitIndex, boolean value )
    {
        if ( value )
        {
            testAndSet( bitIndex );
        }
        else
        {
            clear( bitIndex );
        }
    }

    public void clear( int bitIndex )
    {
        BufferUtils.rangeCheck( bitIndex, 0, bits, "bitIndex" );
        int index = index( bitIndex );
        long mask = 1L << bitIndex;
        long value = words.get( index );
        long newValue = value & ~mask;
        while ( !words.compareAndSet( index, value, newValue ) )
        {
            value = words.get( index );
            newValue = value & ~mask;
        }
    }

    public void reset()
    {
        for ( int i = 0; i < words.length(); i++ )
        {
            words.set( i, 0 );
        }
    }

    public int size()
    {
        return bits;
    }

    public boolean isEmpty()
    {
        return size() - cardinality() == 0;
    }

    public int cardinality()
    {
        int count = 0;
        for ( int i = 0; i < words.length(); i++ )
        {
            count += Long.bitCount( words.get( i ) );
        }
        return count;
    }

    public int firstNotSet()
    {
        return nextNotSet( 0 );
    }

    public int nextNotSet( int bitIndex )
    {
        BufferUtils.rangeCheck( bitIndex, 0, bits, "bitIndex" );
        int index = index( bitIndex );
        for ( int i = index; i < words.length(); i++ )
        {
            if ( Long.bitCount( words.get( i ) ) == 64 )
            {
                continue;
            }

            for ( int bit = 0; bit < 64; bit++ )
            {
                if ( i * 64 + bit >= bits )
                    break;

                if ( ( words.get( i ) & ( 1L << bit ) ) == 0 )
                {
                    return i * 64 + bit;
                }
            }
        }

        if ( bitIndex != 0 )
        {
            return firstNotSet();
        }

        return -1;
    }

    public int firstSet()
    {
        return nextSet( 0 );
    }

    public int nextSet( int bitIndex )
    {
        int index = index( bitIndex );
        for ( int i = index; i < words.length(); i++ )
        {
            if ( Long.bitCount( words.get( i ) ) == 64 )
            {
                continue;
            }

            for ( int bit = 0; bit < 64; bit++ )
            {
                if ( ( words.get( i ) & ( 1L << bit ) ) == 1 )
                {
                    return i * 64 + bit;
                }
            }
        }
        return -1;
    }

    public void and( FixedLengthBitSet bitSet )
    {
        if ( words.length() < bitSet.words.length() )
        {
            throw new IndexOutOfBoundsException( "Bit size of given bitSet is to large: " + bits + " < " + bitSet.bits );
        }

        for ( int i = 0; i < bitSet.words.length(); i++ )
        {
            long value = words.get( i );
            long newValue = value & bitSet.words.get( i );
            while ( words.compareAndSet( i, value, newValue ) )
            {
                value = words.get( i );
                newValue = value & bitSet.words.get( i );
            }
        }
    }

    public void andNot( FixedLengthBitSet bitSet )
    {
        if ( words.length() < bitSet.words.length() )
        {
            throw new IndexOutOfBoundsException( "Bit size of given bitSet is to large: " + bits + " < " + bitSet.bits );
        }

        for ( int i = 0; i < bitSet.words.length(); i++ )
        {
            long value = words.get( i );
            long newValue = value & ~bitSet.words.get( i );
            while ( words.compareAndSet( i, value, newValue ) )
            {
                value = words.get( i );
                newValue = value & ~bitSet.words.get( i );
            }
        }
    }

    public void or( FixedLengthBitSet bitSet )
    {
        if ( words.length() < bitSet.words.length() )
        {
            throw new IndexOutOfBoundsException( "Bit size of given bitSet is to large: " + bits + " < " + bitSet.bits );
        }

        for ( int i = 0; i < bitSet.words.length(); i++ )
        {
            long value = words.get( i );
            long newValue = value | bitSet.words.get( i );
            while ( words.compareAndSet( i, value, newValue ) )
            {
                value = words.get( i );
                newValue = value | bitSet.words.get( i );
            }
        }
    }

    public void xor( FixedLengthBitSet bitSet )
    {
        if ( words.length() < bitSet.words.length() )
        {
            throw new IndexOutOfBoundsException( "Bit size of given bitSet is to large: " + bits + " < " + bitSet.bits );
        }

        for ( int i = 0; i < bitSet.words.length(); i++ )
        {
            long value = words.get( i );
            long newValue = value ^ bitSet.words.get( i );
            while ( words.compareAndSet( i, value, newValue ) )
            {
                value = words.get( i );
                newValue = value ^ bitSet.words.get( i );
            }
        }
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + bits;
        result = prime * result + words.hashCode();
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj == null )
        {
            return false;
        }
        if ( getClass() != obj.getClass() )
        {
            return false;
        }
        FixedLengthBitSet other = (FixedLengthBitSet) obj;
        if ( bits != other.bits )
        {
            return false;
        }
        if ( !words.equals( other.words ) )
        {
            return false;
        }
        return true;
    }

    private int index( int bitIndex )
    {
        return bitIndex / 64;
    }

}
