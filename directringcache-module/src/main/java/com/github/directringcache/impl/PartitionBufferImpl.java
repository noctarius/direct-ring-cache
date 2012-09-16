package com.github.directringcache.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import com.github.directringcache.PartitionBuffer;
import com.github.directringcache.ReadablePartitionBuffer;
import com.github.directringcache.impl.Buffers.PartitionBufferPoolImpl;
import com.github.directringcache.impl.Partition.Slice;

class PartitionBufferImpl implements PartitionBuffer {

	private final PartitionBufferPoolImpl partitionBufferPool;

	private ByteOrder byteOrder;
	private Slice[] slices;
	private long writerIndex = 0;
	private long readerIndex = 0;

	public PartitionBufferImpl(PartitionBufferPoolImpl partitionBufferPool, ByteOrder byteOrder) {
		this.partitionBufferPool = partitionBufferPool;
		this.byteOrder = byteOrder;
		resize(1);
	}

	@Override
	public boolean readable() {
		return writerIndex - readerIndex > 0;
	}

	@Override
	public byte readByte() {
		return read();
	}

	@Override
	public short readUnsignedByte() {
		return (short) (read() & 0xFF);
	}

	@Override
	public byte[] readBytes(byte[] bytes) {
		return readBytes(bytes, 0, bytes.length);
	}

	@Override
	public byte[] readBytes(byte[] bytes, int index, int length) {
		for (int i = index; i < length; i++) {
			bytes[i] = read();
		}
		return bytes;
	}

	@Override
	public char readChar() {
		return (char) readShort();
	}

	@Override
	public double readDouble() {
		return Double.longBitsToDouble(readLong());
	}

	@Override
	public float readFloat() {
		return Float.intBitsToFloat(readInt());
	}

	@Override
	public long readLong() {
		return BufferUtils.getLong(this, byteOrder == ByteOrder.BIG_ENDIAN);
	}

	@Override
	public short readShort() {
		return BufferUtils.getShort(this, byteOrder == ByteOrder.BIG_ENDIAN);
	}

	@Override
	public int readUnsignedShort() {
		return readShort() & 0xFFFF;
	}

	@Override
	public int readInt() {
		return BufferUtils.getInt(this, byteOrder == ByteOrder.BIG_ENDIAN);
	}

	@Override
	public long readUnsignedInt() {
		return readInt() & 0xFFFFFFFFL;
	}

	@Override
	public long readerIndex() {
		return readerIndex;
	}

	@Override
	public void readerIndex(long readerIndex) {
		this.readerIndex = readerIndex;
	}

	@Override
	public boolean writeable() {
		return maxCapacity() - writerIndex > 0;
	}

	@Override
	public void writeByte(int value) {
		put((byte) value);
	}

	@Override
	public void writeBytes(byte[] bytes) {
		writeBytes(bytes, 0, bytes.length);
	}

	@Override
	public void writeBytes(byte[] bytes, int index, int length) {
		for (int i = index; i < length; i++) {
			writeByte(bytes[i]);
		}
	}

	@Override
	public void writeByteBuffer(ByteBuffer buffer) {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeByteBuffer(ByteBuffer buffer, int index, int length) {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeRingBuffer(ReadablePartitionBuffer buffer, long index, long length) {
		// TODO Auto-generated method stub

	}

	@Override
	public void writeChar(int value) {
		writeShort((short) value);
	}

	@Override
	public void writeDouble(double value) {
		writeDouble(Double.doubleToLongBits(value));
	}

	@Override
	public void writeFloat(float value) {
		writeInt(Float.floatToIntBits(value));
	}

	@Override
	public void writeLong(long value) {
		BufferUtils.putLong(value, this, byteOrder == ByteOrder.BIG_ENDIAN);
	}

	@Override
	public void writeShort(short value) {
		BufferUtils.putShort(value, this, byteOrder == ByteOrder.BIG_ENDIAN);
	}

	@Override
	public void writeInt(int value) {
		BufferUtils.putInt(value, this, byteOrder == ByteOrder.BIG_ENDIAN);
	}

	@Override
	public long writerIndex() {
		return writerIndex;
	}

	@Override
	public void writerIndex(long writerIndex) {
		this.writerIndex = writerIndex;
	}

	@Override
	public ByteOrder byteOrder() {
		return byteOrder;
	}

	@Override
	public void byteOrder(ByteOrder byteOrder) {
		this.byteOrder = byteOrder;
	}

	@Override
	public long capacity() {
		long capacity = 0;
		for (Slice slice : slices) {
			capacity += slice != null ? slice.writerIndex() : 0L;
		}
		return capacity;
	}

	@Override
	public long maxCapacity() {
		return slices() * sliceByteSize();
	}

	@Override
	public int sliceByteSize() {
		return partitionBufferPool.getSliceByteSize();
	}

	@Override
	public int slices() {
		int slices = 0;
		for (int i = 0; i < this.slices.length; i++) {
			slices += this.slices[i] != null ? 1 : 0;
		}
		return slices;
	}

	@Override
	public synchronized void free() {
		for (Slice slice : slices) {
			partitionBufferPool.freeSlice(slice);
		}
		Arrays.fill(slices, null);
	}

	private void put(byte value) {
		put(writerIndex++, value);
	}

	private void put(long position, byte value) {
		int sliceIndex = sliceIndex(position);
		if (sliceIndex > slices.length) {
			resize(sliceIndex);
		}
		slices[sliceIndex].put((int) (position % sliceIndex), value);
	}

	private byte read() {
		return read(readerIndex++);
	}

	private byte read(long position) {
		if (position > writerIndex) {
			throw new IndexOutOfBoundsException("Position " + position + " is not readable");
		}
		int sliceIndex = sliceIndex(position);
		return slices[sliceIndex].read((int) (position % sliceIndex));
	}

	private int sliceIndex(long position) {
		return (int) (position / sliceByteSize());
	}

	private synchronized void resize(int newSize) {
		Slice[] temp = new Slice[newSize];
		System.arraycopy(slices, 0, temp, 0, slices.length);
		temp[temp.length] = partitionBufferPool.requestSlice();
		slices = temp;
	}

}
