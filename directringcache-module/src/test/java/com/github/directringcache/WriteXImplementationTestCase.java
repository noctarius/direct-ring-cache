package com.github.directringcache;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.github.directringcache.impl.BufferUtils;
import com.github.directringcache.impl.ByteBufferPartition;
import com.github.directringcache.selector.RoundRobinPartitionSliceSelector;
import com.github.directringcache.spi.PartitionFactory;
import com.github.directringcache.spi.PartitionSliceSelector;

public class WriteXImplementationTestCase {

	@Test
	public void testWriteByte() throws Exception {
		PartitionSliceSelector partitionSliceSelector = new RoundRobinPartitionSliceSelector();
		PartitionFactory partitionFactory = ByteBufferPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY;
		PartitionBufferBuilder builder = new PartitionBufferBuilder(partitionFactory, partitionSliceSelector);
		PartitionBufferPool pool = builder.allocatePool("500M", 50, "256K");

		try {
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			long bytes = BufferUtils.descriptorToByteSize("256K") * 20;

			PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
			assertEquals(BufferUtils.descriptorToByteSize("256K"), partitionBuffer.maxCapacity());
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			for (int i = 0; i < bytes + 1; i++) {
				partitionBuffer.writeByte(7);
			}
			assertEquals(BufferUtils.descriptorToByteSize("256K") * 21, partitionBuffer.maxCapacity());
			assertEquals(BufferUtils.descriptorToByteSize("256K") * 20 + 1, partitionBuffer.capacity());
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			for (int i = 0; i < bytes + 1; i++) {
				assertEquals("Wrong value at position " + i, 7, partitionBuffer.readByte());
			}

			pool.freePartitionBuffer(partitionBuffer);
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

		} finally {
			pool.close();
		}
	}

	@Test
	public void testWriteShort() throws Exception {
		PartitionSliceSelector partitionSliceSelector = new RoundRobinPartitionSliceSelector();
		PartitionFactory partitionFactory = ByteBufferPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY;
		PartitionBufferBuilder builder = new PartitionBufferBuilder(partitionFactory, partitionSliceSelector);
		PartitionBufferPool pool = builder.allocatePool("500M", 50, "256K");

		try {
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			long bytes = BufferUtils.descriptorToByteSize("256K") * 20;

			PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
			assertEquals(BufferUtils.descriptorToByteSize("256K"), partitionBuffer.maxCapacity());
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			for (int i = 0; i < bytes / 2 + 1; i++) {
				partitionBuffer.writeShort((short) 15555);
			}
			assertEquals(BufferUtils.descriptorToByteSize("256K") * 21, partitionBuffer.maxCapacity());
			assertEquals(BufferUtils.descriptorToByteSize("256K") * 20 + 2, partitionBuffer.capacity());
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			for (int i = 0; i < bytes / 2 + 1; i++) {
				assertEquals("Wrong value at position " + i, 15555, partitionBuffer.readShort());
			}

			pool.freePartitionBuffer(partitionBuffer);
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

		} finally {
			pool.close();
		}
	}

	@Test
	public void testWriteInt() throws Exception {
		PartitionSliceSelector partitionSliceSelector = new RoundRobinPartitionSliceSelector();
		PartitionFactory partitionFactory = ByteBufferPartition.DIRECT_BYTEBUFFER_PARTITION_FACTORY;
		PartitionBufferBuilder builder = new PartitionBufferBuilder(partitionFactory, partitionSliceSelector);
		PartitionBufferPool pool = builder.allocatePool("500M", 50, "256K");

		try {
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			long bytes = BufferUtils.descriptorToByteSize("256K") * 20;

			PartitionBuffer partitionBuffer = pool.getPartitionBuffer();
			assertEquals(BufferUtils.descriptorToByteSize("256K"), partitionBuffer.maxCapacity());
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			for (int i = 0; i < bytes / 4 + 1; i++) {
				partitionBuffer.writeInt(755550);
			}
			assertEquals(BufferUtils.descriptorToByteSize("256K") * 21, partitionBuffer.maxCapacity());
			assertEquals(BufferUtils.descriptorToByteSize("256K") * 20 + 4, partitionBuffer.capacity());
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

			for (int i = 0; i < bytes / 4 + 1; i++) {
				assertEquals("Wrong value at position " + i, 755550, partitionBuffer.readInt());
			}

			pool.freePartitionBuffer(partitionBuffer);
			System.out.println("Pool slices " + pool.getSliceCount() + "(" + pool.getAllocatedMemory() + " bytes), unused " + pool.getFreeSliceCount());

		} finally {
			pool.close();
		}
	}

}
