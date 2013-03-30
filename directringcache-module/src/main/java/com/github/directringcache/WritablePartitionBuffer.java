package com.github.directringcache;

import java.nio.ByteBuffer;

public interface WritablePartitionBuffer
{

    boolean writeable();

    void writeByte( int value );

    void writeBytes( byte[] bytes );

    void writeBytes( byte[] bytes, int offset, int length );

    void writeByteBuffer( ByteBuffer byteBuffer );

    void writeByteBuffer( ByteBuffer byteBuffer, int offset, int length );

    void writePartitionBuffer( ReadablePartitionBuffer partitionBuffer );

    void writePartitionBuffer( ReadablePartitionBuffer partitionBuffer, long offset, long length );

    void writeChar( int value );

    void writeDouble( double value );

    void writeFloat( float value );

    void writeLong( long value );

    void writeShort( short value );

    void writeInt( int value );

    long writerIndex();

    void writerIndex( long writerIndex );

}
