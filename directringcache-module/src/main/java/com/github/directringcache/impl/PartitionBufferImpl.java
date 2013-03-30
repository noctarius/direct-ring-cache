package com.github.directringcache.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import com.github.directringcache.PartitionBuffer;
import com.github.directringcache.ReadablePartitionBuffer;
import com.github.directringcache.spi.PartitionSlice;

class PartitionBufferImpl
    implements PartitionBuffer
{

    private final PartitionBufferPoolImpl partitionBufferPool;

    private volatile PartitionSlice[] slices = new PartitionSlice[0];

    private ByteOrder byteOrder;

    private long writerIndex = 0;

    private long readerIndex = 0;

    public PartitionBufferImpl( PartitionBufferPoolImpl partitionBufferPool, ByteOrder byteOrder )
    {
        this.partitionBufferPool = partitionBufferPool;
        this.byteOrder = byteOrder;
        resize( 1 );
    }

    @Override
    public boolean readable()
    {
        return writerIndex - readerIndex > 0;
    }

    @Override
    public long readableSize()
    {
        return writerIndex;
    }

    @Override
    public byte readByte()
    {
        return read();
    }

    @Override
    public short readUnsignedByte()
    {
        return (short) ( read() & 0xFF );
    }

    @Override
    public void readBytes( byte[] bytes )
    {
        readBytes( bytes, 0, bytes.length );
    }

    @Override
    public void readBytes( byte[] bytes, int offset, int length )
    {
        int baseSliceIndex = sliceIndex( readerIndex );
        PartitionSlice slice = slices[baseSliceIndex];
        if ( slice.readableBytes() >= length )
        {
            slice.read( bytes, offset, length );
        }
        else
        {
            int remaining = length - slice.readableBytes();
            int additionalSlices = ( remaining / sliceByteSize() ) + ( remaining % sliceByteSize() != 0 ? 1 : 0 );

            if ( baseSliceIndex + additionalSlices + 1 > slices.length )
            {
                throw new IndexOutOfBoundsException( "Not enough data to load" );
            }

            int sliceOffset = offset;
            for ( int i = baseSliceIndex; i <= baseSliceIndex + additionalSlices; i++ )
            {
                int readable = Math.min( slices[i].readableBytes(), length - sliceOffset );
                slices[i].read( bytes, sliceOffset, readable );
                sliceOffset += readable;
            }
        }
        readerIndex += length;
    }

    @Override
    public char readChar()
    {
        return (char) readShort();
    }

    @Override
    public double readDouble()
    {
        return Double.longBitsToDouble( readLong() );
    }

    @Override
    public float readFloat()
    {
        return Float.intBitsToFloat( readInt() );
    }

    @Override
    public long readLong()
    {
        return BufferUtils.getLong( this, byteOrder == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public short readShort()
    {
        return BufferUtils.getShort( this, byteOrder == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public int readUnsignedShort()
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public int readInt()
    {
        return BufferUtils.getInt( this, byteOrder == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public long readUnsignedInt()
    {
        return readInt() & 0xFFFFFFFFL;
    }

    @Override
    public long readerIndex()
    {
        return readerIndex;
    }

    @Override
    public void readerIndex( long readerIndex )
    {
        this.readerIndex = readerIndex;
    }

    @Override
    public boolean writeable()
    {
        return maxCapacity() - writerIndex > 0;
    }

    @Override
    public void writeByte( int value )
    {
        put( (byte) value );
    }

    @Override
    public void writeBytes( byte[] bytes )
    {
        writeBytes( bytes, 0, bytes.length );
    }

    @Override
    public void writeBytes( byte[] bytes, int offset, int length )
    {
        int baseSliceIndex = slices.length - 1;
        PartitionSlice slice = slices[baseSliceIndex];
        if ( slice.writeableBytes() >= length )
        {
            slice.put( bytes, offset, length );
        }
        else
        {
            int remaining = length - slice.writeableBytes();
            int additionalSlices = ( remaining / sliceByteSize() ) + ( remaining % sliceByteSize() != 0 ? 1 : 0 );

            resize( slices.length + additionalSlices );

            int sliceOffset = offset;
            for ( int i = baseSliceIndex; i <= baseSliceIndex + additionalSlices; i++ )
            {
                int writeable = Math.min( slices[i].writeableBytes(), length - sliceOffset );
                slices[i].put( bytes, sliceOffset, writeable );
                sliceOffset += writeable;
            }
        }
        writerIndex += length;
    }

    @Override
    public void writeByteBuffer( ByteBuffer byteBuffer )
    {
        byteBuffer.mark();
        byteBuffer.position( 0 );

        if ( byteBuffer.hasArray() )
        {
            writeBytes( byteBuffer.array(), byteBuffer.arrayOffset(), byteBuffer.remaining() );
        }
        else
        {
            while ( byteBuffer.hasRemaining() )
            {
                put( byteBuffer.get() );
            }
        }
        byteBuffer.reset();
    }

    @Override
    public void writeByteBuffer( ByteBuffer byteBuffer, int offset, int length )
    {
        byteBuffer.mark();
        byteBuffer.position( offset );
        if ( byteBuffer.hasArray() )
        {
            writeBytes( byteBuffer.array(), byteBuffer.arrayOffset() + offset, length );
        }
        else
        {
            int position = 0;
            while ( position++ < length )
            {
                put( byteBuffer.get() );
            }
        }
        byteBuffer.reset();
    }

    @Override
    public void writePartitionBuffer( ReadablePartitionBuffer partitionBuffer )
    {
        writePartitionBuffer( partitionBuffer, 0, -1 );// TODO
    }

    @Override
    public void writePartitionBuffer( ReadablePartitionBuffer partitionBuffer, long offset, long length )
    {
        long readerIndex = partitionBuffer.readerIndex();
        partitionBuffer.readerIndex( offset );
        long position = 0;
        while ( position++ < length )
        {
            put( partitionBuffer.readByte() );
        }
        partitionBuffer.readerIndex( readerIndex );
    }

    @Override
    public void writeChar( int value )
    {
        writeShort( (short) value );
    }

    @Override
    public void writeDouble( double value )
    {
        writeLong( Double.doubleToLongBits( value ) );
    }

    @Override
    public void writeFloat( float value )
    {
        writeInt( Float.floatToIntBits( value ) );
    }

    @Override
    public void writeLong( long value )
    {
        BufferUtils.putLong( value, this, byteOrder == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public void writeShort( short value )
    {
        BufferUtils.putShort( value, this, byteOrder == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public void writeInt( int value )
    {
        BufferUtils.putInt( value, this, byteOrder == ByteOrder.BIG_ENDIAN );
    }

    @Override
    public long writerIndex()
    {
        return writerIndex;
    }

    @Override
    public void writerIndex( long writerIndex )
    {
        this.writerIndex = writerIndex;
    }

    @Override
    public ByteOrder byteOrder()
    {
        return byteOrder;
    }

    @Override
    public void byteOrder( ByteOrder byteOrder )
    {
        this.byteOrder = byteOrder;
    }

    @Override
    public long capacity()
    {
        long capacity = 0;
        for ( PartitionSlice slice : slices )
        {
            capacity += slice != null ? slice.writerIndex() : 0L;
        }
        return capacity;
    }

    @Override
    public long maxCapacity()
    {
        return slices() * sliceByteSize();
    }

    @Override
    public int sliceByteSize()
    {
        return partitionBufferPool.getSliceByteSize();
    }

    @Override
    public int slices()
    {
        int slices = 0;
        for ( int i = 0; i < this.slices.length; i++ )
        {
            slices += this.slices[i] != null ? 1 : 0;
        }
        return slices;
    }

    @Override
    public synchronized void free()
    {
        for ( PartitionSlice slice : slices )
        {
            partitionBufferPool.freeSlice( slice );
        }
        Arrays.fill( slices, null );
    }

    private void put( byte value )
    {
        put( writerIndex++, value );
    }

    private void put( long position, byte value )
    {
        int sliceIndex = sliceIndex( position );
        if ( sliceIndex >= slices.length )
        {
            resize( sliceIndex + 1 );
        }

        int relativePosition = (int) ( sliceIndex == 0 ? position : position % sliceByteSize() );
        slices[sliceIndex].put( relativePosition, value );
    }

    private byte read()
    {
        return read( readerIndex++ );
    }

    private byte read( long position )
    {
        if ( position > writerIndex )
        {
            throw new IndexOutOfBoundsException( "Position " + position + " is not readable" );
        }
        int sliceIndex = sliceIndex( position );
        return slices[sliceIndex].read( (int) ( sliceIndex == 0 ? position : position % sliceByteSize() ) );
    }

    private int sliceIndex( long position )
    {
        return (int) ( position / sliceByteSize() );
    }

    private synchronized void resize( int newSize )
    {
        int oldSize = slices.length;
        PartitionSlice[] temp = new PartitionSlice[newSize];
        if ( slices != null )
        {
            System.arraycopy( slices, 0, temp, 0, slices.length );
        }
        for ( int i = oldSize; i < newSize; i++ )
        {
            temp[i] = partitionBufferPool.requestSlice();
        }
        if ( temp[temp.length - 1] != null )
        {
            slices = temp;
        }
    }

}
