package com.github.directringcache.impl;

import com.github.directringcache.PartitionBuffer;

final class BufferUtils {

	private static final long KILOBYTE_BYTE_SIZE = 1024 * 1024;
	private static final long MEGABYTE_BYTE_SIZE = KILOBYTE_BYTE_SIZE * 1024;
	private static final long GIGABYTE_BYTE_SIZE = MEGABYTE_BYTE_SIZE * 1024;
	private static final long TERABYTE_BYTE_SIZE = GIGABYTE_BYTE_SIZE * 1024;

	private BufferUtils() {
	}

	static void putShort(short value, PartitionBuffer partitionBuffer, boolean bigEndian) {
		if (bigEndian) {
			partitionBuffer.writeByte((byte) (value >> 8));
			partitionBuffer.writeByte((byte) (value >> 0));
		}
		else {
			partitionBuffer.writeByte((byte) (value >> 0));
			partitionBuffer.writeByte((byte) (value >> 8));
		}
	}

	static short getShort(PartitionBuffer partitionBuffer, boolean bigEndian) {
		if (bigEndian) {
			byte b1 = partitionBuffer.readByte();
			byte b0 = partitionBuffer.readByte();
			return buildShort(b1, b0);
		}
		else {
			byte b0 = partitionBuffer.readByte();
			byte b1 = partitionBuffer.readByte();
			return buildShort(b1, b0);
		}
	}

	static void putInt(int value, PartitionBuffer partitionBuffer, boolean bigEndian) {
		if (bigEndian) {
			partitionBuffer.writeByte((byte) (value >> 24));
			partitionBuffer.writeByte((byte) (value >> 16));
			partitionBuffer.writeByte((byte) (value >> 8));
			partitionBuffer.writeByte((byte) (value >> 0));
		}
		else {
			partitionBuffer.writeByte((byte) (value >> 0));
			partitionBuffer.writeByte((byte) (value >> 8));
			partitionBuffer.writeByte((byte) (value >> 16));
			partitionBuffer.writeByte((byte) (value >> 24));
		}
	}

	static int getInt(PartitionBuffer partitionBuffer, boolean bigEndian) {
		if (bigEndian) {
			byte b3 = partitionBuffer.readByte();
			byte b2 = partitionBuffer.readByte();
			byte b1 = partitionBuffer.readByte();
			byte b0 = partitionBuffer.readByte();
			return buildInt(b3, b2, b1, b0);
		}
		else {
			byte b0 = partitionBuffer.readByte();
			byte b1 = partitionBuffer.readByte();
			byte b2 = partitionBuffer.readByte();
			byte b3 = partitionBuffer.readByte();
			return buildInt(b3, b2, b1, b0);
		}
	}

	static void putLong(long value, PartitionBuffer partitionBuffer, boolean bigEndian) {
		if (bigEndian) {
			partitionBuffer.writeByte((byte) (value >> 56));
			partitionBuffer.writeByte((byte) (value >> 48));
			partitionBuffer.writeByte((byte) (value >> 40));
			partitionBuffer.writeByte((byte) (value >> 32));
			partitionBuffer.writeByte((byte) (value >> 24));
			partitionBuffer.writeByte((byte) (value >> 16));
			partitionBuffer.writeByte((byte) (value >> 8));
			partitionBuffer.writeByte((byte) (value >> 0));
		}
		else {
			partitionBuffer.writeByte((byte) (value >> 0));
			partitionBuffer.writeByte((byte) (value >> 8));
			partitionBuffer.writeByte((byte) (value >> 16));
			partitionBuffer.writeByte((byte) (value >> 24));
			partitionBuffer.writeByte((byte) (value >> 32));
			partitionBuffer.writeByte((byte) (value >> 40));
			partitionBuffer.writeByte((byte) (value >> 48));
			partitionBuffer.writeByte((byte) (value >> 56));
		}
	}

	static long getLong(PartitionBuffer partitionBuffer, boolean bigEndian) {
		if (bigEndian) {
			byte b7 = partitionBuffer.readByte();
			byte b6 = partitionBuffer.readByte();
			byte b5 = partitionBuffer.readByte();
			byte b4 = partitionBuffer.readByte();
			byte b3 = partitionBuffer.readByte();
			byte b2 = partitionBuffer.readByte();
			byte b1 = partitionBuffer.readByte();
			byte b0 = partitionBuffer.readByte();
			return buildLong(b7, b6, b5, b4, b3, b2, b1, b0);
		}
		else {
			byte b0 = partitionBuffer.readByte();
			byte b1 = partitionBuffer.readByte();
			byte b2 = partitionBuffer.readByte();
			byte b3 = partitionBuffer.readByte();
			byte b4 = partitionBuffer.readByte();
			byte b5 = partitionBuffer.readByte();
			byte b6 = partitionBuffer.readByte();
			byte b7 = partitionBuffer.readByte();
			return buildLong(b7, b6, b5, b4, b3, b2, b1, b0);
		}
	}

	static void rangeCheck(int position, int lowerBound, int upperBound, String name) {
		if (position < lowerBound) {
			throw new IndexOutOfBoundsException("Given value " + name + " is smaller than lower bound " + lowerBound);
		}
		if (position > upperBound) {
			throw new IndexOutOfBoundsException("Given value " + name + " is larger than upper bound " + upperBound);
		}
	}

	static long descriptorToByteSize(String descriptor) {
		// Trim possible whitespaces
		descriptor = descriptor.trim();

		char descriptorChar = descriptor.charAt(descriptor.length() - 1);
		if (Character.isDigit(descriptorChar)) {
			throw new IllegalArgumentException("Descriptor char " + descriptorChar + " is no legal size descriptor (only B, K, M, G, T allowed)");
		}

		for (int i = 0; i < descriptor.length() - 2; i++) {
			if (!Character.isDigit(descriptor.charAt(i))) {
				throw new IllegalArgumentException("Non digit character at position " + i);
			}
		}

		long value = Long.parseLong(descriptor.substring(0, descriptor.length() - 2));
		switch (descriptorChar) {
			case 'b':
			case 'B':
				return value;

			case 'k':
			case 'K':
				return value * KILOBYTE_BYTE_SIZE;

			case 'm':
			case 'M':
				return value * MEGABYTE_BYTE_SIZE;

			case 'g':
			case 'G':
				return value * GIGABYTE_BYTE_SIZE;

			case 't':
			case 'T':
				return value * TERABYTE_BYTE_SIZE;
		}

		throw new IllegalArgumentException("Descriptor character " + descriptorChar + " is unknown (only B, K, M, G, T allowed)");
	}

	private static short buildShort(byte b1, byte b0) {
		return (short) ((((b1 & 0xFF) << 8) | ((b0 & 0xFF) << 0)));
	}

	private static int buildInt(byte b3, byte b2, byte b1, byte b0) {
		return (int) ((((b3 & 0xFF) << 24) | ((b2 & 0xFF) << 16) | ((b1 & 0xFF) << 8) | ((b0 & 0xFF) << 0)));
	}

	private static long buildLong(byte b7, byte b6, byte b5, byte b4, byte b3, byte b2, byte b1, byte b0) {
		return (long) ((((b7 & 0xFF) << 56) | ((b6 & 0xFF) << 48) | ((b5 & 0xFF) << 40) | ((b4 & 0xFF) << 32) | ((b3 & 0xFF) << 24) | ((b2 & 0xFF) << 16)
				| ((b1 & 0xFF) << 8) | ((b0 & 0xFF) << 0)));
	}

}
