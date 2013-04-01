package com.github.directringcache.impl;

import java.nio.ByteBuffer;

import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSliceSelector;

public class ByteBufferPooledPartition
    extends AbstractPartition
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

    private final ByteBufferPartitionSlice[] slices;

    private ByteBufferPooledPartition( int partitionIndex, int slices, int sliceByteSize, boolean directMemory,
                                       PartitionSliceSelector partitionSliceSelector )
    {
        super( partitionIndex, slices, sliceByteSize, partitionSliceSelector, true );

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
    protected AbstractPartitionSlice get( int index )
    {
        return slices[index];
    }

    public class ByteBufferPartitionSlice
        extends AbstractPartitionSlice
    {

        private final ByteBuffer byteBuffer;

        private int writerIndex;

        private int readerIndex;

        private ByteBufferPartitionSlice( ByteBuffer byteBuffer, int index )
        {
            super( index );

            this.byteBuffer = byteBuffer;
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

        @Override
        protected void free()
        {
            // TODO explicitly free if DirectByteBuffer
        }
    }

}
