package com.github.directringcache;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.github.directringcache.impl.FixedLengthBitSet;

public class FixedLengthBitSetTestCase {

	@Test
	public void testRanOutOfBits() throws Exception {
		FixedLengthBitSet bitSet = new FixedLengthBitSet(100);
		for (int i = 0; i < 100; i++) {
			int index = bitSet.nextNotSet(i);
			bitSet.set(index);
			assertEquals(index, i);
			assertEquals(bitSet.cardinality(), i + 1);
		}

		assertEquals(-1, bitSet.nextNotSet(0));
	}

}
