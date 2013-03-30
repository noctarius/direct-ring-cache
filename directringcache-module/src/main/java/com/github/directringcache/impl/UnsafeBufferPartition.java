package com.github.directringcache.impl;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
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

        private static final int BYTE_ARRAY_OFFSET;

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
            BYTE_ARRAY_OFFSET = unsafe != null ? unsafe.arrayBaseOffset( byte[].class ) : -1;
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

    private final long lastMemoryPointer;

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
        this.lastMemoryPointer = basePointer + allocatedLength - 1;

        /*
         * System.out.println( "malloc data: partitionIndex=" + partitionIndex + ", basePointer=" + basePointer +
         * ", allocatedLength=" + allocatedLength + ", lastBytePointer=" + lastMemoryPointer );
         */

        for ( int i = 0; i < slices; i++ )
        {
            this.slices[i] = new UnsafePartitionSlice( i );
            /*
             * System.out.println( "sliced data: memoryPointer=" + ( basePointer + this.slices[i].baseIndex ) +
             * ", sliceIndex=" + i + ", length=" + sliceByteSize + ", sliceOffset=" + this.slices[i].baseIndex +
             * ", lastBytePointer=" + this.slices[i].lastMemoryPointer );
             */
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
        int retry = 0;
        while ( retry++ < 5 )
        {
            int possibleMatch = nextSlice();
            if ( possibleMatch == -1 )
            {
                return null;
            }

            int oldIndex = index.get();
            while ( !index.compareAndSet( oldIndex, possibleMatch ) )
            {
                oldIndex = index.get();
                possibleMatch = nextSlice();
            }
            synchronized ( usedSlices )
            {
                if ( !usedSlices.get( possibleMatch ) )
                {
                    usedSlices.set( possibleMatch );
                    return slices[possibleMatch].lock();
                }
            }
        }
        return null;
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
            partitionSliceSelector.freePartitionSlice( this, partitionIndex, partitionSlice.unlock() );
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

        private final long memoryPointer;

        private final long lastMemoryPointer;

        private final AtomicBoolean lock = new AtomicBoolean( false );

        private int writerIndex;

        private int readerIndex;

        private UnsafePartitionSlice( int index )
        {
            this.index = index;
            this.baseIndex = index * sliceByteSize;
            this.memoryPointer = basePointer + this.baseIndex;
            this.lastMemoryPointer = memoryPointer + sliceByteSize - 1;
            clear();
        }

        @Override
        public void clear()
        {
            unsafe.setMemory( memoryPointer, sliceByteSize, (byte) 0 );
            writerIndex = 0;
            readerIndex = 0;
        }

        @Override
        public void put( byte value )
        {
            put( writerIndex++, value );
        }

        @Override
        public void put( int position, byte value )
        {
            // System.out.println( "writeToOffset=" + ( memoryPointer + position ) );
            unsafe.putByte( memoryPointer + position, value );
        }

        @Override
        public void put( byte[] array, int offset, int length )
        {
            if ( writerAddress() + length - 1 > lastMemoryPointer )
            {
                throw new IndexOutOfBoundsException( "Writing the array exhausted the available size" );
            }
            /*
             * System.out.println( "partition=" + partitionIndex + ", sliceIndex=" + index + ", sliceOffset=" +
             * baseIndex + ", basePointer=" + basePointer + ", allocatedLength=" + allocatedLength + ", writerIndex=" +
             * writerIndex + ", offset=" + offset + ", arrayLength=" + array.length + ", writeLength=" + length +
             * ", writerAddress=" + ( memoryPointer + writerIndex ) );
             */
            unsafe.copyMemory( array, UnsafeUtil.BYTE_ARRAY_OFFSET + offset, null, memoryPointer + writerIndex, length );
            writerIndex += length;
        }

        @Override
        public byte read()
        {
            return read( readerIndex++ );
        }

        @Override
        public byte read( int position )
        {
            return unsafe.getByte( memoryPointer + position );
        }

        @Override
        public void read( byte[] array, int offset, int length )
        {
            if ( readerAddress() + length - 1 > lastMemoryPointer )
            {
                throw new IndexOutOfBoundsException( "Reading the array exhausted the available size" );
            }
            /*
             * System.out.println( "partition=" + partitionIndex + ", sliceIndex=" + index + ", sliceOffset=" +
             * baseIndex + ", basePointer=" + basePointer + ", allocatedLength=" + allocatedLength + ", readerIndex=" +
             * readerIndex + ", offset=" + offset + ", arrayLength=" + array.length + ", readLength=" + length +
             * ", readerAddress=" + ( memoryPointer + readerIndex ) );
             */
            unsafe.copyMemory( null, memoryPointer + readerIndex, array, UnsafeUtil.BYTE_ARRAY_OFFSET + offset, length );
            readerIndex += length;
        }

        @Override
        public int getSliceByteSize()
        {
            return sliceByteSize;
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

        private synchronized PartitionSlice lock()
        {
            if ( !lock.compareAndSet( false, true ) )
            {
                throw new IllegalStateException( "PartitionSlice already locked" );
            }
            return this;
        }

        private synchronized PartitionSlice unlock()
        {
            if ( !lock.compareAndSet( true, false ) )
            {
                throw new IllegalStateException( "PartitionSlice not locked" );
            }
            return this;
        }

        private long readerAddress()
        {
            return memoryPointer + readerIndex;
        }

        private long writerAddress()
        {
            return memoryPointer + writerIndex;
        }
    }

}
