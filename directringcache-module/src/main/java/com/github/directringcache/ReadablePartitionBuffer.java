package com.github.directringcache;

public interface ReadablePartitionBuffer {

	boolean readable();

	byte readByte();

	short readUnsignedByte();

	byte[] readBytes(byte[] bytes);

	byte[] readBytes(byte[] bytes, int index, int length);

	char readChar();

	double readDouble();

	float readFloat();

	long readLong();

	short readShort();

	int readUnsignedShort();

	int readInt();

	long readUnsignedInt();

	long readerIndex();

	void readerIndex(long readerIndex);

}
