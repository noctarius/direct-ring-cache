package com.github.directringcache;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.github.directringcache.impl.BufferUtils;
import com.github.directringcache.impl.ByteBufferPartition;
import com.github.directringcache.impl.UnsafeBufferPartition;
import com.github.directringcache.selector.RoundRobinPartitionSliceSelector;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSliceSelector;

@RunWith(Parameterized.class)
public class PartitionBufferTestCase {

	@Parameters
	public static Collection<Object[]> parameters() {
		return Arrays.asList(new Object[][] { { ByteBufferPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY },
				{ ByteBufferPartition.HEAP_BYTEBUFFER_PARTITION_FACTORY }, { UnsafeBufferPartition.UNSAFE_PARTITION_FACTORY } });
	}

	private final PartitionFactory partitionFactory;

	public PartitionBufferTestCase(PartitionFactory partitionFactory) {
		this.partitionFactory = partitionFactory;
	}

	@Test
	public void testAllocation() throws Exception {
		PartitionSliceSelector partitionSliceSelector = new RoundRobinPartitionSliceSelector();
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

	@Test
	public void testAllocation2() throws Exception {
		PartitionSliceSelector partitionSliceSelector = new RoundRobinPartitionSliceSelector();
		PartitionBufferBuilder builder = new PartitionBufferBuilder(partitionFactory, partitionSliceSelector);
		PartitionBufferPool pool = builder.allocatePool("500M", 50, "256K");

		try {
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			long bytes = BufferUtils.descriptorToByteSize("256K") * 20;

			PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
			assertEquals(BufferUtils.descriptorToByteSize("256K"), partitionBuffer.maxCapacity());
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			for (int i = 0; i < bytes + 1; i++) {
				partitionBuffer.writeByte(1);
			}
			assertEquals(BufferUtils.descriptorToByteSize("256K") * 21, partitionBuffer.maxCapacity());
			assertEquals(BufferUtils.descriptorToByteSize("256K") * 20 + 1, partitionBuffer.capacity());
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			for (int i = 0; i < bytes + 1; i++) {
				assertEquals("Wrong value at position " + i, 1, partitionBuffer.readByte());
			}

			pool.freePartitionBuffer(partitionBuffer);
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

		} finally {
			pool.close();
		}
	}

	@Test(expected = RuntimeException.class)
	public void testAllocationBufferFull() throws Exception {
		PartitionSliceSelector partitionSliceSelector = new RoundRobinPartitionSliceSelector();
		PartitionBufferBuilder builder = new PartitionBufferBuilder(partitionFactory, partitionSliceSelector);
		PartitionBufferPool pool = builder.allocatePool("1M", 1, "256K");

		try {
			for (int o = 0; o < 100; o++) {
				System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

				long bytes = BufferUtils.descriptorToByteSize("256K") * 20;

				PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
				assertEquals(BufferUtils.descriptorToByteSize("256K"), partitionBuffer.maxCapacity());
				System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

				for (int i = 0; i < bytes + 1; i++) {
					partitionBuffer.writeByte(1);
				}
				assertEquals(BufferUtils.descriptorToByteSize("256K") * 21, partitionBuffer.maxCapacity());
				assertEquals(BufferUtils.descriptorToByteSize("256K") * 20 + 1, partitionBuffer.capacity());
				System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

				for (int i = 0; i < bytes + 1; i++) {
					assertEquals("Wrong value at position " + i, 1, partitionBuffer.readByte());
				}

				pool.freePartitionBuffer(partitionBuffer);
				System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());
			}
		} finally {
			pool.close();
		}
	}

	@Test
	public void testAllocationRoundRobinFullRound() throws Exception {
		PartitionSliceSelector partitionSliceSelector = new RoundRobinPartitionSliceSelector();
		PartitionBufferBuilder builder = new PartitionBufferBuilder(partitionFactory, partitionSliceSelector);
		PartitionBufferPool pool = builder.allocatePool("10M", 5, "256K");

		try {
			for (int o = 0; o < 10; o++) {
				System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

				long bytes = BufferUtils.descriptorToByteSize("256K") * 20;

				PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
				assertEquals(BufferUtils.descriptorToByteSize("256K"), partitionBuffer.maxCapacity());
				System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

				for (int i = 0; i < bytes + 1; i++) {
					partitionBuffer.writeByte(1);
				}
				assertEquals(BufferUtils.descriptorToByteSize("256K") * 21, partitionBuffer.maxCapacity());
				assertEquals(BufferUtils.descriptorToByteSize("256K") * 20 + 1, partitionBuffer.capacity());
				System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

				for (int i = 0; i < bytes + 1; i++) {
					assertEquals("Wrong value at position " + i, 1, partitionBuffer.readByte());
				}

				pool.freePartitionBuffer(partitionBuffer);
				System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());
			}
		} finally {
			pool.close();
		}
	}

}
