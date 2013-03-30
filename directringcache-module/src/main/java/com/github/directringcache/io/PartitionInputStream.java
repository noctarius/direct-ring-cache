package com.github.directringcache.io;

import java.io.IOException;
import java.io.InputStream;

import com.github.directringcache.PartitionBuffer;

public class PartitionInputStream
    extends InputStream
{

    private final PartitionBuffer partitionBuffer;

    private long markedPosition = 0;

    public PartitionInputStream( PartitionBuffer partitionBuffer )
    {
        this.partitionBuffer = partitionBuffer;
    }

    @Override
    public int read()
        throws IOException
    {
        return partitionBuffer.readByte();
    }

    @Override
    public void close()
        throws IOException
    {
        super.close();
        partitionBuffer.free();
    }

    @Override
    public int read( byte[] bytes )
        throws IOException
    {
        partitionBuffer.readBytes( bytes );
        return bytes.length;
    }

    @Override
    public int read( byte[] bytes, int offset, int length )
        throws IOException
    {
        partitionBuffer.readBytes( bytes, offset, length );
        return length;
    }

    @Override
    public int available()
        throws IOException
    {
        return (int) ( partitionBuffer.writerIndex() - partitionBuffer.readerIndex() );
    }

    @Override
    public synchronized void mark( int readLimit )
    {
        this.markedPosition = partitionBuffer.readerIndex();
    }

    @Override
    public synchronized void reset()
        throws IOException
    {
        partitionBuffer.readerIndex( markedPosition );
    }

    @Override
    public boolean markSupported()
    {
        return true;
    }

}
