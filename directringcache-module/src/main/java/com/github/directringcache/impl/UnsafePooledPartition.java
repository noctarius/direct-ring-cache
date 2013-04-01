package com.github.directringcache.impl;

import java.lang.reflect.Field;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSliceSelector;

@SuppressWarnings( "restriction" )
public class UnsafePooledPartition
    extends AbstractPartition
{

    public static final PartitionFactory UNSAFE_POOLED_PARTITION_FACTORY = new PartitionFactory()
    {

        @Override
        public Partition newPartition( int partitionIndex, int sliceByteSize, int slices,
                                       PartitionSliceSelector partitionSliceSelector )
        {
            return new UnsafePooledPartition( partitionIndex, slices, sliceByteSize, partitionSliceSelector );
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

    private static final Logger LOGGER = LoggerFactory.getLogger( UnsafePooledPartition.class );

    private static final Logger SLICE_LOGGER = LoggerFactory.getLogger( UnsafePartitionSlice.class );

    private final sun.misc.Unsafe unsafe = UnsafeUtil.getUnsafe();

    private final long allocatedLength;

    private final UnsafePartitionSlice[] slices;

    private UnsafePooledPartition( int partitionIndex, int slices, int sliceByteSize,
                                   PartitionSliceSelector partitionSliceSelector )
    {
        super( partitionIndex, slices, sliceByteSize, partitionSliceSelector, true );

        this.slices = new UnsafePartitionSlice[slices];
        this.allocatedLength = sliceByteSize * slices;

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "malloc data: partitionIndex=" + partitionIndex + ", allocatedLength=" + allocatedLength );
        }

        for ( int i = 0; i < slices; i++ )
        {
            this.slices[i] = new UnsafePartitionSlice( i );

            if ( LOGGER.isTraceEnabled() )
            {
                UnsafePartitionSlice slice = this.slices[i];
                LOGGER.trace( "sliced data: memoryPointer=" + ( slice.memoryPointer ) + ", sliceIndex=" + i
                    + ", length=" + sliceByteSize + ", lastBytePointer=" + slice.lastMemoryPointer );
            }
        }
    }

    @Override
    protected AbstractPartitionSlice get( int index )
    {
        return slices[index];
    }

    public class UnsafePartitionSlice
        extends AbstractPartitionSlice
    {

        private final long memoryPointer;

        private final long lastMemoryPointer;

        private volatile int writerIndex;

        private volatile int readerIndex;

        private UnsafePartitionSlice( int index )
        {
            super( index );

            this.memoryPointer = unsafe.allocateMemory( sliceByteSize );
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
            if ( SLICE_LOGGER.isTraceEnabled() )
            {
                SLICE_LOGGER.trace( "writeToOffset=" + ( memoryPointer + position ) );
            }
            unsafe.putByte( memoryPointer + position, value );
        }

        @Override
        public void put( byte[] array, int offset, int length )
        {
            if ( writerAddress() + length - 1 > lastMemoryPointer )
            {
                throw new IndexOutOfBoundsException( "Writing the array exhausted the available size" );
            }

            if ( SLICE_LOGGER.isTraceEnabled() )
            {
                SLICE_LOGGER.trace( "partition=" + partitionIndex + ", sliceIndex=" + index + ", allocatedLength="
                    + allocatedLength + ", writerIndex=" + writerIndex + ", offset=" + offset + ", arrayLength="
                    + array.length + ", writeLength=" + length + ", writerAddress=" + ( memoryPointer + writerIndex ) );
            }
            long memOffset = memoryPointer + readerIndex;
            unsafe.copyMemory( array, UnsafeUtil.BYTE_ARRAY_OFFSET + offset, null, memOffset, length );
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

            if ( SLICE_LOGGER.isTraceEnabled() )
            {
                SLICE_LOGGER.trace( "partition=" + partitionIndex + ", sliceIndex=" + index + ", allocatedLength="
                    + allocatedLength + ", readerIndex=" + readerIndex + ", offset=" + offset + ", arrayLength="
                    + array.length + ", readLength=" + length + ", readerAddress=" + ( memoryPointer + readerIndex ) );
            }
            long memOffset = memoryPointer + readerIndex;
            unsafe.copyMemory( null, memOffset, array, UnsafeUtil.BYTE_ARRAY_OFFSET + offset, length );
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
            return UnsafePooledPartition.this;
        }

        private long readerAddress()
        {
            return memoryPointer + readerIndex;
        }

        private long writerAddress()
        {
            return memoryPointer + writerIndex;
        }

        @Override
        protected void free()
        {
            unsafe.setMemory( memoryPointer, sliceByteSize, (byte) 0 );
            unsafe.freeMemory( memoryPointer );
        }
    }

}
