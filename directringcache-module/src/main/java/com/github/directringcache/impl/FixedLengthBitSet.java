package com.github.directringcache.impl;

import java.util.Arrays;

public class FixedLengthBitSet {

	private final int bits;

	private final long[] words;

	public FixedLengthBitSet(int bits) {
		this.bits = bits;
		this.words = new long[bits % 64 == 0 ? bits / 64 : bits / 64 + 1];
	}

	public void flip(int bitIndex) {
		BufferUtils.rangeCheck(bitIndex, 0, bits, "bitIndex");
		int index = index(bitIndex);
		words[index] ^= (1L << bitIndex);
	}

	public boolean get(int bitIndex) {
		BufferUtils.rangeCheck(bitIndex, 0, bits, "bitIndex");
		int index = index(bitIndex);
		return (words[index] & (1L << bitIndex)) != 0;
	}

	public void set(int bitIndex) {
		BufferUtils.rangeCheck(bitIndex, 0, bits, "bitIndex");
		int index = index(bitIndex);
		words[index] |= (1L << bitIndex);
	}

	public void set(int bitIndex, boolean value) {
		if (value) {
			set(bitIndex);
		} else {
			clear(bitIndex);
		}
	}

	public void clear(int bitIndex) {
		BufferUtils.rangeCheck(bitIndex, 0, bits, "bitIndex");
		int index = index(bitIndex);
		words[index] &= ~(1L << bitIndex);
	}

	public void reset() {
		for (int i = 0; i < words.length; i++) {
			words[i] = 0;
		}
	}

	public int size() {
		return bits;
	}

	public boolean isEmpty() {
		return size() - cardinality() == 0;
	}

	public int cardinality() {
		int count = 0;
		for (int i = 0; i < words.length; i++) {
			count += Long.bitCount(words[i]);
		}
		return count;
	}

	public int firstNotSet() {
		return nextNotSet(0);
	}

	public int nextNotSet(int bitIndex) {
		int index = index(bitIndex);
		for (int i = index; i < words.length; i++) {
			if (Long.bitCount(words[i]) == 64) {
				continue;
			}

			for (int bit = 0; bit < 64; bit++) {
				if ((words[i] & (1L << bit)) == 0) {
					return i * 64 + bit;
				}
			}
		}
		return -1;
	}

	public int firstSet() {
		return nextSet(0);
	}

	public int nextSet(int bitIndex) {
		int index = index(bitIndex);
		for (int i = index; i < words.length; i++) {
			if (Long.bitCount(words[i]) == 64) {
				continue;
			}

			for (int bit = 0; bit < 64; bit++) {
				if ((words[i] & (1L << bit)) == 1) {
					return i * 64 + bit;
				}
			}
		}
		return -1;
	}

	public void and(FixedLengthBitSet bitSet) {
		if (words.length < bitSet.words.length) {
			throw new IndexOutOfBoundsException("Bit size of given bitSet is to large: " + bits + " < " + bitSet.bits);
		}

		for (int i = 0; i < bitSet.words.length; i++) {
			words[i] &= bitSet.words[i];
		}
	}

	public void andNot(FixedLengthBitSet bitSet) {
		if (words.length < bitSet.words.length) {
			throw new IndexOutOfBoundsException("Bit size of given bitSet is to large: " + bits + " < " + bitSet.bits);
		}

		for (int i = 0; i < bitSet.words.length; i++) {
			words[i] &= ~bitSet.words[i];
		}
	}

	public void or(FixedLengthBitSet bitSet) {
		if (words.length < bitSet.words.length) {
			throw new IndexOutOfBoundsException("Bit size of given bitSet is to large: " + bits + " < " + bitSet.bits);
		}

		for (int i = 0; i < bitSet.words.length; i++) {
			words[i] |= bitSet.words[i];
		}
	}

	public void xor(FixedLengthBitSet bitSet) {
		if (words.length < bitSet.words.length) {
			throw new IndexOutOfBoundsException("Bit size of given bitSet is to large: " + bits + " < " + bitSet.bits);
		}

		for (int i = 0; i < bitSet.words.length; i++) {
			words[i] ^= bitSet.words[i];
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + bits;
		result = prime * result + Arrays.hashCode(words);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		FixedLengthBitSet other = (FixedLengthBitSet) obj;
		if (bits != other.bits) {
			return false;
		}
		if (!Arrays.equals(words, other.words)) {
			return false;
		}
		return true;
	}

	private int index(int bitIndex) {
		return bitIndex / 64;
	}

}
