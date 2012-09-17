package com.github.directringcache;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

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
		PartitionBufferPool pool = builder.allocatePool("1M", 2, "2K");

		try {
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
			assertEquals(1024 * 1024 * 2, partitionBuffer.maxCapacity());
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			for (int i = 0; i < 1024 * 1024 + 1; i++) {
				partitionBuffer.writeByte(1);
			}
			assertEquals(1024 * 1024 * 2, partitionBuffer.maxCapacity());
			assertEquals(1024 * 1024 + 1, partitionBuffer.capacity());
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			pool.freePartitionBuffer(partitionBuffer);
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

		} finally {
			pool.close();
		}
	}
}
