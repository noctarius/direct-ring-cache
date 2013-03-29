package com.github.directringcache;

import java.nio.ByteOrder;

public interface PartitionBuffer
    extends ReadablePartitionBuffer, WritablePartitionBuffer
{

    long capacity();

    long maxCapacity();

    int sliceByteSize();

    int slices();

    void free();

    ByteOrder byteOrder();

    void byteOrder( ByteOrder byteOrder );

}
