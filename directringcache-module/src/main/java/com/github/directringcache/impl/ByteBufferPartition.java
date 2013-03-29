package com.github.directringcache.impl;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSlice;

public class ByteBufferPartition implements Partition {

	public static final PartitionFactory DIRECT_BYTEBUFFER_PARTITION_FACTORY = new PartitionFactory() {

		@Override
		public Partition newPartition(int sliceByteSize, int slices) {
			return new ByteBufferPartition(slices, sliceByteSize, true);
		}
	};

	public static final PartitionFactory HEAP_BYTEBUFFER_PARTITION_FACTORY = new PartitionFactory() {

		@Override
		public Partition newPartition(int sliceByteSize, int slices) {
			return new ByteBufferPartition(slices, sliceByteSize, false);
		}
	};

	private final ByteBuffer byteBuffer;
	private final ByteBufferPartitionSlice[] slices;
	private final int sliceByteSize;
	private final FixedLengthBitSet usedSlices;
	private final AtomicInteger index = new AtomicInteger(0);

	private ByteBufferPartition(int slices, int sliceByteSize, boolean directMemory) {
		this.sliceByteSize = sliceByteSize;
		this.usedSlices = new FixedLengthBitSet(slices);
		this.slices = new ByteBufferPartitionSlice[slices];
		this.byteBuffer = directMemory ? ByteBuffer.allocateDirect(sliceByteSize * slices) : ByteBuffer.allocate(sliceByteSize * slices);

		for (int i = 0; i < slices; i++) {
			this.slices[i] = new ByteBufferPartitionSlice(i);
		}
	}

	@Override
	public int available() {
		return usedSlices.size() - usedSlices.cardinality();
	}

	@Override
	public int used() {
		return usedSlices.cardinality();
	}

	@Override
	public int getSliceByteSize() {
		return sliceByteSize;
	}

	@Override
	public PartitionSlice get() {
		int possibleMatch = nextSlice();
		if (possibleMatch == -1) {
			return null;
		}

		while (!index.compareAndSet(index.get(), possibleMatch)) {
			possibleMatch = nextSlice();
			if (possibleMatch == -1) {
				return null;
			}
		}
		synchronized (usedSlices) {
			usedSlices.set(possibleMatch);
		}
		return slices[possibleMatch];
	}

	@Override
	public void free(PartitionSlice slice) {
		if (!(slice instanceof ByteBufferPartitionSlice)) {
			throw new IllegalArgumentException("Given slice cannot be handled by this PartitionBufferPool");
		}
		ByteBufferPartitionSlice partitionSlice = (ByteBufferPartitionSlice) slice;
		if (partitionSlice.getPartition() != this) {
			throw new IllegalArgumentException("Given slice cannot be handled by this PartitionBufferPool");
		}
		synchronized (usedSlices) {
			usedSlices.clear(partitionSlice.index);
		}
		slice.clear();
	}

	@Override
	public void close() {
		byteBuffer.clear();
	}

	private int nextSlice() {
		if (usedSlices.isEmpty()) {
			return -1;
		}

		int index = this.index.get();
		return index == 0 ? usedSlices.firstNotSet() : usedSlices.nextNotSet(index + 1);
	}

	public class ByteBufferPartitionSlice implements PartitionSlice {

		private final int index;
		private final int baseIndex;
		private int writerIndex;
		private int readerIndex;

		private ByteBufferPartitionSlice(int index) {
			this.index = index;
			this.baseIndex = index * sliceByteSize;
		}

		@Override
		public void clear() {
			for (int i = 0; i < sliceByteSize; i++) {
				byteBuffer.put(baseIndex + i, (byte) 0);
			}
			writerIndex = 0;
			readerIndex = 0;
		}

		@Override
		public void put(byte value) {
			put(writerIndex - baseIndex, value);
		}

		@Override
		public void put(int position, byte value) {
			byteBuffer.put(baseIndex + position, value);
			writerIndex++;
		}

		@Override
		public byte read() {
			return read(readerIndex - baseIndex);
		}

		@Override
		public byte read(int position) {
			readerIndex++;
			return byteBuffer.get(baseIndex + position);
		}

		@Override
		public int readableBytes() {
			return writerIndex - readerIndex;
		}

		@Override
		public int writeableBytes() {
			return sliceByteSize - writerIndex;
		}

		@Override
		public int writerIndex() {
			return writerIndex;
		}

		@Override
		public int readerIndex() {
			return readerIndex;
		}

		@Override
		public void writerIndex(int writerIndex) {
			BufferUtils.rangeCheck(writerIndex, 0, sliceByteSize, "writerIndex");
			this.writerIndex = writerIndex;
		}

		@Override
		public void readerIndex(int readerIndex) {
			BufferUtils.rangeCheck(readerIndex, 0, sliceByteSize, "readerIndex");
			this.readerIndex = readerIndex;
		}

		@Override
		public Partition getPartition() {
			return ByteBufferPartition.this;
		}
	}

}
