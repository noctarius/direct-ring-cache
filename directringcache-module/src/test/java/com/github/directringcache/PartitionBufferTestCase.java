package com.github.directringcache;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.github.directringcache.impl.BufferUtils;
import com.github.directringcache.impl.ByteBufferPartition;
import com.github.directringcache.selector.RoundRobinPartitionSliceSelector;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSliceSelector;

public class PartitionBufferTestCase {

	@Test
	public void testAllocation() throws Exception {
		PartitionSliceSelector partitionSliceSelector = new RoundRobinPartitionSliceSelector();
		PartitionFactory partitionFactory = ByteBufferPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY;
		PartitionBufferBuilder builder = new PartitionBufferBuilder(partitionFactory, partitionSliceSelector);
		PartitionBufferPool pool = builder.allocatePool("500M", 50, "512K");

		try {
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
			assertEquals(BufferUtils.descriptorToByteSize("512k"), partitionBuffer.maxCapacity());
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			for (int i = 0; i < 1024 * 1024 + 1; i++) {
				partitionBuffer.writeByte(1);
			}
			assertEquals(BufferUtils.descriptorToByteSize("512k") * 3, partitionBuffer.maxCapacity());
			assertEquals(BufferUtils.descriptorToByteSize("512k") * 2 + 1, partitionBuffer.capacity());
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			pool.freePartitionBuffer(partitionBuffer);
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

		} finally {
			pool.close();
		}
	}
}
