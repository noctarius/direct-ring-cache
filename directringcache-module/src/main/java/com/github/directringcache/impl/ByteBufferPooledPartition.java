package com.github.directringcache.impl;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSlice;
import com.github.directringcache.spi.PartitionSliceSelector;

public class ByteBufferPooledPartition
    implements Partition
{

    public static final PartitionFactory DIRECT_BYTEBUFFER_PARTITION_FACTORY = new PartitionFactory()
    {

        @Override
        public Partition newPartition( int partitionIndex, int sliceByteSize, int slices,
                                       PartitionSliceSelector partitionSliceSelector )
        {
            return new ByteBufferPooledPartition( partitionIndex, slices, sliceByteSize, true, partitionSliceSelector );
        }
    };

    public static final PartitionFactory HEAP_BYTEBUFFER_PARTITION_FACTORY = new PartitionFactory()
    {

        @Override
        public Partition newPartition( int partitionIndex, int sliceByteSize, int slices,
                                       PartitionSliceSelector partitionSliceSelector )
        {
            return new ByteBufferPooledPartition( partitionIndex, slices, sliceByteSize, false, partitionSliceSelector );
        }
    };

    private final PartitionSliceSelector partitionSliceSelector;

    private final int partitionIndex;

    private final ByteBufferPartitionSlice[] slices;

    private final int sliceByteSize;

    private final FixedLengthBitSet usedSlices;

    private ByteBufferPooledPartition( int partitionIndex, int slices, int sliceByteSize, boolean directMemory,
                                       PartitionSliceSelector partitionSliceSelector )
    {
        this.partitionSliceSelector = partitionSliceSelector;
        this.partitionIndex = partitionIndex;
        this.sliceByteSize = sliceByteSize;
        this.usedSlices = new FixedLengthBitSet( slices );
        this.slices = new ByteBufferPartitionSlice[slices];

        for ( int i = 0; i < slices; i++ )
        {
            ByteBuffer buffer =
                directMemory ? ByteBuffer.allocateDirect( sliceByteSize * slices )
                                : ByteBuffer.allocate( sliceByteSize );
            this.slices[i] = new ByteBufferPartitionSlice( buffer, i );
        }
    }

    @Override
    public boolean isPooled()
    {
        return true;
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

            synchronized ( usedSlices )
            {
                if ( !usedSlices.get( possibleMatch ) )
                {
                    if ( usedSlices.testAndSet( possibleMatch ) )
                    {
                        return slices[possibleMatch].lock();
                    }
                }
            }
        }
        return null;
    }

    @Override
    public int getSliceCount()
    {
        return usedSlices.size();
    }

    @Override
    public void free( PartitionSlice slice )
    {
        if ( !( slice instanceof ByteBufferPartitionSlice ) )
        {
            throw new IllegalArgumentException( "Given slice cannot be handled by this PartitionBufferPool" );
        }
        ByteBufferPartitionSlice partitionSlice = (ByteBufferPartitionSlice) slice;
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
    public void close()
    {
        synchronized ( usedSlices )
        {
            for ( int i = 0; i < slices.length; i++ )
            {
                partitionSliceSelector.freePartitionSlice( this, partitionIndex, slices[i] );
            }
        }
    }

    private int nextSlice()
    {
        if ( usedSlices.isEmpty() )
        {
            return -1;
        }

        return usedSlices.firstNotSet();
    }

    public class ByteBufferPartitionSlice
        implements PartitionSlice
    {

        private final ByteBuffer byteBuffer;

        private final int index;

        private final AtomicBoolean lock = new AtomicBoolean( false );

        private int writerIndex;

        private int readerIndex;

        private ByteBufferPartitionSlice( ByteBuffer byteBuffer, int index )
        {
            this.byteBuffer = byteBuffer;
            this.index = index;
        }

        @Override
        public void clear()
        {
            byteBuffer.clear();
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
            byteBuffer.put( position, value );
        }

        @Override
        public void put( byte[] array, int offset, int length )
        {
            for ( int i = 0; i < length; i++ )
            {
                put( writerIndex++, array[offset + i] );
            }
        }

        @Override
        public byte read()
        {
            return read( readerIndex++ );
        }

        @Override
        public byte read( int position )
        {
            return byteBuffer.get( position );
        }

        @Override
        public void read( byte[] array, int offset, int length )
        {
            for ( int i = 0; i < length; i++ )
            {
                array[offset + i] = read( readerIndex++ );
            }
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
            return ByteBufferPooledPartition.this;
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
    }

}
