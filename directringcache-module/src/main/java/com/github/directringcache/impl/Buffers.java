package com.github.directringcache.impl;

import com.github.directringcache.PartitionBuffer;
import com.github.directringcache.PartitionBufferPool;
import com.github.directringcache.impl.Partition.Slice;

public final class Buffers {

	private Buffers() {
	}

	public static PartitionBufferPool allocateByAllocatedMemorySizeDescriptor(String allocatedMemorySizeDescriptor, int partitions, int slicesPerPartition) {
		long allocatedMemoryByteSize = BufferUtils.descriptorToByteSize(allocatedMemorySizeDescriptor);
		return allocateByAllocatedMemoryByteSize(allocatedMemoryByteSize, partitions, slicesPerPartition);
	}

	public static PartitionBufferPool allocateByAllocatedMemoryByteSize(long allocatedMemoryByteSize, int partitions, int slicesPerPartition) {
		if (allocatedMemoryByteSize % partitions != 0) {
			throw new IllegalArgumentException("partitions is not a divisor of allocatedMemoryByteSize");
		}
		if (!isPowerOfTwo(allocatedMemoryByteSize)) {
			throw new IllegalArgumentException("allocatedMemoryByteSize is not a power of 2");
		}
		long partitionByteSize = allocatedMemoryByteSize / partitions;
		return allocateByPartitionByteSize(partitionByteSize, partitions, slicesPerPartition);
	}

	public static PartitionBufferPool allocateByPartitioSizenDescriptor(String partitionSizeDescriptor, int partitions, int slicesPerPartition) {
		long partitionByteSize = BufferUtils.descriptorToByteSize(partitionSizeDescriptor);
		return allocateByPartitionByteSize(partitionByteSize, partitions, slicesPerPartition);
	}

	public static PartitionBufferPool allocateByPartitionByteSize(long partitionByteSize, int partitions, int slicesPerPartition) {
		if (partitionByteSize % slicesPerPartition != 0) {
			throw new IllegalArgumentException("slicesPerPartition is not a divisor of partitionByteSize");
		}
		if (!isPowerOfTwo(partitionByteSize)) {
			throw new IllegalArgumentException("partitionByteSize is not a power of 2");
		}
		long sliceByteSize = partitionByteSize / slicesPerPartition;
		if (sliceByteSize > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Bytesize per slice will be a value larger than allowed slice maximum");
		}
		return allocateBySliceByteSize((int) sliceByteSize, partitions, slicesPerPartition);
	}

	public static PartitionBufferPool allocateBySliceSizeDescriptor(String sliceSizeDescriptor, int partitions, int slicesPerPartition) {
		long sliceByteSize = BufferUtils.descriptorToByteSize(sliceSizeDescriptor);
		if (sliceByteSize > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("sliceSizeDescriptor defines a value larger than allowed slice maximum");
		}
		return allocateBySliceByteSize((int) sliceByteSize, partitions, slicesPerPartition);
	}

	public static PartitionBufferPool allocateBySliceByteSize(int sliceByteSize, int partitions, int slicesPerPartition) {
		if (!isPowerOfTwo(sliceByteSize)) {
			throw new IllegalArgumentException("sliceByteSize is not a power of 2");
		}
		return new PartitionBufferPoolImpl(partitions, sliceByteSize, slicesPerPartition);
	}

	private static boolean isPowerOfTwo(long value) {
		return value > 0 && ((value & (value - 1)) != 0);
	}

	static final class PartitionBufferPoolImpl implements PartitionBufferPool {

		private final Partition[] partitions;
		private final int sliceByteSize;
		private final int slices;

		private PartitionBufferPoolImpl(int partitions, int sliceByteSize, int slices) {
			this.partitions = new Partition[partitions];
			this.sliceByteSize = sliceByteSize;
			this.slices = slices;

			// Initialize partitions
			for (int i = 0; i < partitions; i++) {
				this.partitions[i] = new Partition(slices, sliceByteSize);
			}
		}

		Slice requestSlice() {
			// TODO: Implement slice choosing algorithm
			return null;
		}

		void freeSlice(Slice slice) {
			if (slice != null) {
				slice.getPartition().free(slice);
			}
		}

		@Override
		public PartitionBuffer getPartitionBuffer() {
			return null;
		}

		@Override
		public void freePartitionBuffer(PartitionBuffer partitionBuffer) {
			partitionBuffer.free();
		}

		public long getAllocatedMemory() {
			return getPartitionCount() * getPartitionByteSize();
		}

		public int getPartitionByteSize() {
			return getSliceCountPerPartition() * getSliceByteSize();
		}

		public int getPartitionCount() {
			return partitions.length;
		}

		public int getSliceCountPerPartition() {
			return slices;
		}

		public int getSliceCount() {
			return getSliceCountPerPartition() * getPartitionCount();
		}

		public int getSliceByteSize() {
			return sliceByteSize;
		}

		public int getUsedSliceCount() {
			int usedSlices = 0;
			for (Partition partition : partitions) {
				usedSlices += partition.used();
			}
			return usedSlices;
		}

		public int getFreeSliceCount() {
			int available = 0;
			for (Partition partition : partitions) {
				available += partition.available();
			}
			return available;
		}
	}

}
