package com.github.directringcache.io;

import java.io.DataInputStream;

import com.github.directringcache.PartitionBuffer;

public class PartitionDataInputStream extends DataInputStream {

	public PartitionDataInputStream(PartitionBuffer partitionBuffer) {
		super(new PartitionInputStream(partitionBuffer));
	}

}
