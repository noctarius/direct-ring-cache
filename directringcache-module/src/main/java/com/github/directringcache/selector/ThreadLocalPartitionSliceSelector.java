package com.github.directringcache.selector;

import java.util.Random;

import com.github.directringcache.spi.Partition;
import com.github.directringcache.spi.PartitionSlice;
import com.github.directringcache.spi.PartitionSliceSelector;

public class ThreadLocalPartitionSliceSelector implements PartitionSliceSelector {

	private final ThreadLocal<Partition> partitionAssignment = new ThreadLocal<Partition>();
	private final Random random = new Random(-System.currentTimeMillis());

	private volatile boolean[] assigned;

	@Override
	public PartitionSlice selectPartitionSlice(Partition[] partitions) {
		Partition partition = partitionAssignment.get();
		if (partition != null && partition.available() > 0) {
			return partition.get();
		}

		synchronized (this) {
			if (assigned == null) {
				assigned = new boolean[partitions.length];
			}

			for (int i = 0; i < assigned.length; i++) {
				if (!assigned[i]) {
					assigned[i] = true;
					partition = partitions[i];
					partitionAssignment.set(partition);
					PartitionSlice slice = partition.get();
					if (slice != null) {
						return slice;
					}
				}
			}

			int index = random.nextInt(partitions.length);
			PartitionSlice slice = partitions[index].get();
			if (slice != null) {
				return slice;
			}
		}

		throw new RuntimeException("Could not retrieve a new partition slice");
	}
}
