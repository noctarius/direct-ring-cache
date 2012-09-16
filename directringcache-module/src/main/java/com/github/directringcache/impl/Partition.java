package com.github.directringcache.impl;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

class Partition {

	private final ByteBuffer byteBuffer;
	private final Slice[] slices;
	private final int sliceByteSize;
	private final FixedLengthBitSet usedSlices;
	private final AtomicInteger index = new AtomicInteger(0);

	Partition(int slices, int sliceByteSize) {
		this.sliceByteSize = sliceByteSize;
		this.usedSlices = new FixedLengthBitSet(slices);
		this.slices = new Slice[slices];
		this.byteBuffer = ByteBuffer.allocateDirect(sliceByteSize * slices);
	}

	public int available() {
		return usedSlices.size() - usedSlices.cardinality();
	}

	public int used() {
		return usedSlices.cardinality();
	}

	public int getSliceByteSize() {
		return sliceByteSize;
	}

	public Slice get() {
		int possibleMatch = nextSlice();
		while (!index.compareAndSet(index.get(), possibleMatch)) {
			possibleMatch = nextSlice();
		}
		synchronized (usedSlices) {
			usedSlices.set(possibleMatch);
		}
		return slices[possibleMatch];
	}

	public void free(Slice slice) {
		synchronized (usedSlices) {
			usedSlices.clear(slice.index);
		}
		slice.clear();
	}

	private int nextSlice() {
		int index = this.index.get();
		int pos = index == 0 ? -1 : usedSlices.nextNotSet(index + 1);
		if (pos == -1) {
			pos = usedSlices.firstNotSet();
		}
		return pos;
	}

	public class Slice {

		private final int index;
		private final int baseIndex;
		private int writerIndex;
		private int readerIndex;

		private Slice(int index) {
			this.index = index;
			this.baseIndex = index * sliceByteSize;
		}

		public void clear() {
			for (int i = 0; i < sliceByteSize; i++) {
				byteBuffer.put(baseIndex + i, (byte) 0);
			}
			writerIndex = 0;
			readerIndex = 0;
		}

		public void put(byte value) {
			put(writerIndex++, value);
		}

		public void put(int position, byte value) {
			byteBuffer.put(baseIndex + position, value);
		}

		public byte read() {
			return read(readerIndex++);
		}

		public byte read(int position) {
			return byteBuffer.get(baseIndex + position);
		}

		public int readableBytes() {
			return writerIndex - readerIndex;
		}

		public int writeableBytes() {
			return sliceByteSize - writerIndex;
		}

		public int writerIndex() {
			return writerIndex;
		}

		public int readerIndex() {
			return readerIndex;
		}

		public void writerIndex(int writerIndex) {
			BufferUtils.rangeCheck(writerIndex, 0, sliceByteSize, "writerIndex");
			this.writerIndex = writerIndex;
		}

		public void readerIndex(int readerIndex) {
			BufferUtils.rangeCheck(readerIndex, 0, sliceByteSize, "readerIndex");
			this.readerIndex = readerIndex;
		}

		Partition getPartition() {
			return Partition.this;
		}
	}

}
