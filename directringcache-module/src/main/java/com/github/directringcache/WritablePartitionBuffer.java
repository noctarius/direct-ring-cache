package com.github.directringcache;

import java.nio.ByteBuffer;

public interface WritablePartitionBuffer {

	boolean writeable();

	void writeByte(int value);

	void writeBytes(byte[] bytes);

	void writeBytes(byte[] bytes, int index, int length);

	void writeByteBuffer(ByteBuffer buffer);

	void writeByteBuffer(ByteBuffer buffer, int index, int length);

	void writeRingBuffer(ReadablePartitionBuffer buffer, long index, long length);

	void writeChar(int value);

	void writeDouble(double value);

	void writeFloat(float value);

	void writeLong(long value);

	void writeShort(short value);

	void writeInt(int value);

	long writerIndex();

	void writerIndex(long writerIndex);

}
