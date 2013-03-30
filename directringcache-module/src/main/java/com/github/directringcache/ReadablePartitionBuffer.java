package com.github.directringcache;

public interface ReadablePartitionBuffer
{

    boolean readable();

    long readableSize();

    byte readByte();

    short readUnsignedByte();

    void readBytes( byte[] bytes );

    void readBytes( byte[] bytes, int offset, int length );

    char readChar();

    double readDouble();

    float readFloat();

    long readLong();

    short readShort();

    int readUnsignedShort();

    int readInt();

    long readUnsignedInt();

    long readerIndex();

    void readerIndex( long readerIndex );

}
