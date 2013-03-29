package com.github.directringcache.impl;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSlice;
import com.github.directringcache.spi.PartitionSliceSelector;

@SuppressWarnings( "restriction" )
public class UnsafeBufferPartition
    implements Partition
{

    public static final PartitionFactory UNSAFE_PARTITION_FACTORY = new PartitionFactory()
    {

        @Override
        public Partition newPartition( int partitionIndex, int sliceByteSize, int slices,
                                       PartitionSliceSelector partitionSliceSelector )
        {
            return new UnsafeBufferPartition( partitionIndex, slices, sliceByteSize, partitionSliceSelector );
        }
    };

    public static class UnsafeUtil
    {

        private static final sun.misc.Unsafe UNSAFE;

        static
        {
            sun.misc.Unsafe unsafe;
            try
            {
                Field unsafeField = sun.misc.Unsafe.class.getDeclaredField( "theUnsafe" );
                unsafeField.setAccessible( true );
                unsafe = (sun.misc.Unsafe) unsafeField.get( null );
            }
            catch ( Exception e )
            {
                unsafe = null;
            }

            UNSAFE = unsafe;
        }

        private UnsafeUtil()
        {
        }

        public static sun.misc.Unsafe getUnsafe()
        {
            return UNSAFE;
        }
    }

    private final sun.misc.Unsafe unsafe = UnsafeUtil.getUnsafe();

    private final PartitionSliceSelector partitionSliceSelector;

    private final int partitionIndex;

    private final long basePointer;

    private final long allocatedLength;

    private final UnsafePartitionSlice[] slices;

    private final int sliceByteSize;

    private final FixedLengthBitSet usedSlices;

    private final AtomicInteger index = new AtomicInteger( 0 );

    private UnsafeBufferPartition( int partitionIndex, int slices, int sliceByteSize,
                                   PartitionSliceSelector partitionSliceSelector )
    {
        this.partitionSliceSelector = partitionSliceSelector;
        this.partitionIndex = partitionIndex;
        this.sliceByteSize = sliceByteSize;
        this.usedSlices = new FixedLengthBitSet( slices );
        this.slices = new UnsafePartitionSlice[slices];
        this.allocatedLength = sliceByteSize * slices;
        this.basePointer = unsafe.allocateMemory( allocatedLength );

        for ( int i = 0; i < slices; i++ )
        {
            this.slices[i] = new UnsafePartitionSlice( i );
        }
    }

    @Override
    public int available()
    {
        return usedSlices.size() - usedSlices.cardinality();
    }

    @Override
    public int used()
    {
        return usedSlices.cardinality();
    }

    @Override
    public int getSliceByteSize()
    {
        return sliceByteSize;
    }

    @Override
    public PartitionSlice get()
    {
        int possibleMatch = nextSlice();
        if ( possibleMatch == -1 )
        {
            return null;
        }

        while ( !index.compareAndSet( index.get(), possibleMatch ) )
        {
            possibleMatch = nextSlice();
        }
        synchronized ( usedSlices )
        {
            usedSlices.set( possibleMatch );
        }
        return slices[possibleMatch];
    }

    @Override
    public void free( PartitionSlice slice )
    {
        if ( !( slice instanceof UnsafePartitionSlice ) )
        {
            throw new IllegalArgumentException( "Given slice cannot be handled by this PartitionBufferPool" );
        }
        UnsafePartitionSlice partitionSlice = (UnsafePartitionSlice) slice;
        if ( partitionSlice.getPartition() != this )
        {
            throw new IllegalArgumentException( "Given slice cannot be handled by this PartitionBufferPool" );
        }
        synchronized ( usedSlices )
        {
            usedSlices.clear( partitionSlice.index );
            partitionSliceSelector.freePartitionSlice( this, partitionIndex, slice );
        }
        slice.clear();
    }

    @Override
    public int getSliceCount()
    {
        return usedSlices.size();
    }

    @Override
    public void close()
    {
        synchronized ( usedSlices )
        {
            for ( int i = 0; i < slices.length; i++ )
            {
                partitionSliceSelector.freePartitionSlice( this, partitionIndex, slices[i] );
            }
        }

        unsafe.setMemory( basePointer, allocatedLength, (byte) 0 );
        unsafe.freeMemory( basePointer );
    }

    private int nextSlice()
    {
        if ( usedSlices.isEmpty() )
        {
            return -1;
        }

        int index = this.index.get();
        int pos = index == 0 || index == usedSlices.size() - 1 ? -1 : usedSlices.nextNotSet( index + 1 );
        if ( pos == -1 )
        {
            pos = usedSlices.firstNotSet();
        }

        return pos;
    }

    public class UnsafePartitionSlice
        implements PartitionSlice
    {

        private final int index;

        private final int baseIndex;

        private int writerIndex;

        private int readerIndex;

        private UnsafePartitionSlice( int index )
        {
            this.index = index;
            this.baseIndex = index * sliceByteSize;
            clear();
        }

        @Override
        public void clear()
        {
            unsafe.setMemory( basePointer + baseIndex, sliceByteSize, (byte) 0 );
            writerIndex = 0;
            readerIndex = 0;
        }

        @Override
        public void put( byte value )
        {
            put( writerIndex - baseIndex, value );
        }

        @Override
        public void put( int position, byte value )
        {
            unsafe.putByte( basePointer + baseIndex + position, value );
            writerIndex++;
        }

        @Override
        public void put( byte[] array, int offset, int length )
        {
            for ( int i = offset; i < offset + length; i++ )
            {
                unsafe.putByte( basePointer + baseIndex + writerIndex, array[i] );
                writerIndex++;
            }
        }

        @Override
        public byte read()
        {
            return read( readerIndex - baseIndex );
        }

        @Override
        public byte read( int position )
        {
            readerIndex++;
            return unsafe.getByte( basePointer + baseIndex + position );
        }

        @Override
        public void read( byte[] array, int offset, int length )
        {
            for ( int i = offset; i < offset + length; i++ )
            {
                array[i] = unsafe.getByte( basePointer + baseIndex + readerIndex );
                readerIndex++;
            }
        }

        @Override
        public int readableBytes()
        {
            return writerIndex - readerIndex;
        }

        @Override
        public int writeableBytes()
        {
            return sliceByteSize - writerIndex;
        }

        @Override
        public int writerIndex()
        {
            return writerIndex;
        }

        @Override
        public int readerIndex()
        {
            return readerIndex;
        }

        @Override
        public void writerIndex( int writerIndex )
        {
            BufferUtils.rangeCheck( writerIndex, 0, sliceByteSize, "writerIndex" );
            this.writerIndex = writerIndex;
        }

        @Override
        public void readerIndex( int readerIndex )
        {
            BufferUtils.rangeCheck( readerIndex, 0, sliceByteSize, "readerIndex" );
            this.readerIndex = readerIndex;
        }

        @Override
        public Partition getPartition()
        {
            return UnsafeBufferPartition.this;
        }
    }

}
