package com.github.directringcache.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.directringcache.PartitionBuffer;

@SuppressWarnings( "restriction" )
public final class BufferUtils
{

    private static final Logger LOGGER = LoggerFactory.getLogger( BufferUtils.class );

    private static final long KILOBYTE_BYTE_SIZE = 1024;

    private static final long MEGABYTE_BYTE_SIZE = KILOBYTE_BYTE_SIZE * 1024;

    private static final long GIGABYTE_BYTE_SIZE = MEGABYTE_BYTE_SIZE * 1024;

    private static final long TERABYTE_BYTE_SIZE = GIGABYTE_BYTE_SIZE * 1024;

    private static final Method DIRECT_BYTE_BUFFER_CLEAN;

    private static final Method DIRECT_BYTE_BUFFER_CLEANER;

    private static final boolean CLEANER_AVAILABLE;

    private static final sun.misc.Unsafe UNSAFE;

    private static final boolean UNSAFE_AVAILABLE;

    static final int BYTE_ARRAY_OFFSET;

    static
    {
        Method directByteBufferClean = null;
        Method directByteBufferCleaner = null;
        try
        {
            Class<?> clazz = Class.forName( "java.nio.DirectByteBuffer" );
            directByteBufferCleaner = clazz.getDeclaredMethod( "cleaner" );

            clazz = Class.forName( "sun.misc.Cleaner" );
            directByteBufferClean = clazz.getDeclaredMethod( "clean" );
        }
        catch ( Exception e )
        {
            // Ignore since DirectByteBuffer or clean() method aren't available on this JVM
        }
        DIRECT_BYTE_BUFFER_CLEAN = directByteBufferClean;
        DIRECT_BYTE_BUFFER_CLEANER = directByteBufferCleaner;
        CLEANER_AVAILABLE = DIRECT_BYTE_BUFFER_CLEAN != null && DIRECT_BYTE_BUFFER_CLEANER != null;

        sun.misc.Unsafe unsafe = null;
        try
        {
            Field unsafeField = sun.misc.Unsafe.class.getDeclaredField( "theUnsafe" );
            unsafeField.setAccessible( true );
            unsafe = (sun.misc.Unsafe) unsafeField.get( null );
        }
        catch ( Exception e )
        {
            // Ignore thins sun.misc.Unsafe doesn't seem to exists on this JVM
        }

        UNSAFE = unsafe;
        UNSAFE_AVAILABLE = UNSAFE != null;
        BYTE_ARRAY_OFFSET = UNSAFE_AVAILABLE ? UNSAFE.arrayBaseOffset( byte[].class ) : -1;
    }

    private BufferUtils()
    {
    }

    static boolean isUnsafeAvailable()
    {
        return UNSAFE_AVAILABLE;
    }

    static sun.misc.Unsafe getUnsafe()
    {
        return UNSAFE;
    }

    static void cleanByteBuffer( ByteBuffer byteBuffer )
    {
        if ( !byteBuffer.isDirect() || CLEANER_AVAILABLE )
        {
            return;
        }

        try
        {
            Object cleaner = DIRECT_BYTE_BUFFER_CLEANER.invoke( byteBuffer );
            DIRECT_BYTE_BUFFER_CLEAN.invoke( cleaner );
        }
        catch ( Exception e )
        {
            LOGGER.debug( "DirectByteBuffer could not be cleaned", e );
        }
    }

    static void putShort( short value, PartitionBuffer partitionBuffer, boolean bigEndian )
    {
        if ( bigEndian )
        {
            partitionBuffer.writeByte( (byte) ( value >> 8 ) );
            partitionBuffer.writeByte( (byte) ( value >> 0 ) );
        }
        else
        {
            partitionBuffer.writeByte( (byte) ( value >> 0 ) );
            partitionBuffer.writeByte( (byte) ( value >> 8 ) );
        }
    }

    static short getShort( PartitionBuffer partitionBuffer, boolean bigEndian )
    {
        if ( bigEndian )
        {
            byte b1 = partitionBuffer.readByte();
            byte b0 = partitionBuffer.readByte();
            return buildShort( b1, b0 );
        }
        else
        {
            byte b0 = partitionBuffer.readByte();
            byte b1 = partitionBuffer.readByte();
            return buildShort( b1, b0 );
        }
    }

    static void putInt( int value, PartitionBuffer partitionBuffer, boolean bigEndian )
    {
        if ( bigEndian )
        {
            partitionBuffer.writeByte( (byte) ( value >>> 24 ) );
            partitionBuffer.writeByte( (byte) ( value >>> 16 ) );
            partitionBuffer.writeByte( (byte) ( value >>> 8 ) );
            partitionBuffer.writeByte( (byte) ( value >>> 0 ) );
        }
        else
        {
            partitionBuffer.writeByte( (byte) ( value >>> 0 ) );
            partitionBuffer.writeByte( (byte) ( value >>> 8 ) );
            partitionBuffer.writeByte( (byte) ( value >>> 16 ) );
            partitionBuffer.writeByte( (byte) ( value >>> 24 ) );
        }
    }

    static int getInt( PartitionBuffer partitionBuffer, boolean bigEndian )
    {
        if ( bigEndian )
        {
            byte b3 = partitionBuffer.readByte();
            byte b2 = partitionBuffer.readByte();
            byte b1 = partitionBuffer.readByte();
            byte b0 = partitionBuffer.readByte();
            return buildInt( b3, b2, b1, b0 );
        }
        else
        {
            byte b0 = partitionBuffer.readByte();
            byte b1 = partitionBuffer.readByte();
            byte b2 = partitionBuffer.readByte();
            byte b3 = partitionBuffer.readByte();
            return buildInt( b3, b2, b1, b0 );
        }
    }

    static void putLong( long value, PartitionBuffer partitionBuffer, boolean bigEndian )
    {
        if ( bigEndian )
        {
            partitionBuffer.writeByte( (byte) ( value >> 56 ) );
            partitionBuffer.writeByte( (byte) ( value >> 48 ) );
            partitionBuffer.writeByte( (byte) ( value >> 40 ) );
            partitionBuffer.writeByte( (byte) ( value >> 32 ) );
            partitionBuffer.writeByte( (byte) ( value >> 24 ) );
            partitionBuffer.writeByte( (byte) ( value >> 16 ) );
            partitionBuffer.writeByte( (byte) ( value >> 8 ) );
            partitionBuffer.writeByte( (byte) ( value >> 0 ) );
        }
        else
        {
            partitionBuffer.writeByte( (byte) ( value >> 0 ) );
            partitionBuffer.writeByte( (byte) ( value >> 8 ) );
            partitionBuffer.writeByte( (byte) ( value >> 16 ) );
            partitionBuffer.writeByte( (byte) ( value >> 24 ) );
            partitionBuffer.writeByte( (byte) ( value >> 32 ) );
            partitionBuffer.writeByte( (byte) ( value >> 40 ) );
            partitionBuffer.writeByte( (byte) ( value >> 48 ) );
            partitionBuffer.writeByte( (byte) ( value >> 56 ) );
        }
    }

    static long getLong( PartitionBuffer partitionBuffer, boolean bigEndian )
    {
        if ( bigEndian )
        {
            byte b7 = partitionBuffer.readByte();
            byte b6 = partitionBuffer.readByte();
            byte b5 = partitionBuffer.readByte();
            byte b4 = partitionBuffer.readByte();
            byte b3 = partitionBuffer.readByte();
            byte b2 = partitionBuffer.readByte();
            byte b1 = partitionBuffer.readByte();
            byte b0 = partitionBuffer.readByte();
            return buildLong( b7, b6, b5, b4, b3, b2, b1, b0 );
        }
        else
        {
            byte b0 = partitionBuffer.readByte();
            byte b1 = partitionBuffer.readByte();
            byte b2 = partitionBuffer.readByte();
            byte b3 = partitionBuffer.readByte();
            byte b4 = partitionBuffer.readByte();
            byte b5 = partitionBuffer.readByte();
            byte b6 = partitionBuffer.readByte();
            byte b7 = partitionBuffer.readByte();
            return buildLong( b7, b6, b5, b4, b3, b2, b1, b0 );
        }
    }

    static void rangeCheck( int position, int lowerBound, int upperBound, String name )
    {
        if ( position < lowerBound )
        {
            throw new IndexOutOfBoundsException( "Given value " + name + " is smaller than lower bound " + lowerBound );
        }
        if ( position >= upperBound )
        {
            throw new IndexOutOfBoundsException( "Given value " + name + " is larger than upper bound " + upperBound );
        }
    }

    public static boolean isPowerOfTwo( long value )
    {
        return value > 0 && ( ( ~value ) & 1 ) == 1;
    }

    public static long descriptorToByteSize( String descriptor )
    {
        // Trim possible whitespaces
        descriptor = descriptor.trim();

        char descriptorChar = descriptor.charAt( descriptor.length() - 1 );
        if ( Character.isDigit( descriptorChar ) )
        {
            throw new IllegalArgumentException( "Descriptor char " + descriptorChar
                + " is no legal size descriptor (only B, K, M, G, T allowed)" );
        }

        for ( int i = 0; i < descriptor.length() - 2; i++ )
        {
            if ( !Character.isDigit( descriptor.charAt( i ) ) )
            {
                throw new IllegalArgumentException( "Non digit character at position " + i );
            }
        }

        double value = Double.parseDouble( descriptor.substring( 0, descriptor.length() - 1 ) );
        switch ( descriptorChar )
        {
            case 'b':
            case 'B':
                return Double.valueOf( value ).longValue();

            case 'k':
            case 'K':
                return Double.valueOf( value * KILOBYTE_BYTE_SIZE ).longValue();

            case 'm':
            case 'M':
                return Double.valueOf( value * MEGABYTE_BYTE_SIZE ).longValue();

            case 'g':
            case 'G':
                return Double.valueOf( value * GIGABYTE_BYTE_SIZE ).longValue();

            case 't':
            case 'T':
                return Double.valueOf( value * TERABYTE_BYTE_SIZE ).longValue();
        }

        throw new IllegalArgumentException( "Descriptor character " + descriptorChar
            + " is unknown (only B, K, M, G, T allowed)" );
    }

    private static short buildShort( byte b1, byte b0 )
    {
        return (short) ( ( ( ( b1 & 0xFF ) << 8 ) | ( ( b0 & 0xFF ) << 0 ) ) );
    }

    private static int buildInt( byte b3, byte b2, byte b1, byte b0 )
    {
        return ( ( ( ( b3 & 0xFF ) << 24 ) | ( ( b2 & 0xFF ) << 16 ) | ( ( b1 & 0xFF ) << 8 ) | ( ( b0 & 0xFF ) << 0 ) ) );
    }

    private static long buildLong( byte b7, byte b6, byte b5, byte b4, byte b3, byte b2, byte b1, byte b0 )
    {
        return ( ( ( ( b7 & 0xFF ) << 56 ) | ( ( b6 & 0xFF ) << 48 ) | ( ( b5 & 0xFF ) << 40 ) | ( ( b4 & 0xFF ) << 32 )
            | ( ( b3 & 0xFF ) << 24 ) | ( ( b2 & 0xFF ) << 16 ) | ( ( b1 & 0xFF ) << 8 ) | ( ( b0 & 0xFF ) << 0 ) ) );
    }

}
