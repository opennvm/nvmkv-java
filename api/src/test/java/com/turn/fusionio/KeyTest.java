/**
 * Copyright (C) 2012 Turn, Inc.  All Rights Reserved.
 * Proprietary and confidential.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * - Neither the name Turn, Inc. nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without specific
 *   prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.turn.fusionio;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.testng.annotations.Test;

/**
 * Tests for the FusionIO API Key class.
 *
 * @author mpetazzoni
 */
@Test
public class KeyTest {

	private static final int KEY_SIZE = 42;

	public void testEmptyConstructorSize() {
		assert new Key().size() == 0;
	}

	public void testEmptyConstructorByteBufferIsNull() {
		assert new Key().getByteBuffer() == null;
	}

	@Test(expectedExceptions=IllegalArgumentException.class)
	public void testAllocateNegativeSizeThrowsException() {
		Key.get(-KEY_SIZE);
	}

	@Test(expectedExceptions=IllegalArgumentException.class)
	public void testAllocateZeroSizeThrowsException() {
		Key.get(0);
	}

	@Test(expectedExceptions=IllegalArgumentException.class)
	public void testAllocateOverSizeThrowsException() {
		Key.get(FusionIOAPI.FUSION_IO_MAX_KEY_SIZE+1);
	}

	public void testAllocatedKeySize() {
		assert Key.get(KEY_SIZE).size() == 42;
	}

	public void testAllocatedKeyByteBuffer() {
		Key k = Key.get(KEY_SIZE);
		assert k.size() == KEY_SIZE;
		assert k.getByteBuffer() != null;
		assert k.getByteBuffer().position() == 0;
		assert k.getByteBuffer().capacity() == k.size();
		assert k.getByteBuffer().limit() == k.size();
		assert k.getByteBuffer().order() == ByteOrder.nativeOrder();
	}

	@Test(expectedExceptions=IllegalStateException.class)
	public void testGetLongValueFromInvalidKeySizeThrowsException() {
		Key.get(7).longValue();
	}

	public void testGetLongValueFromEmptyKeyBuffer() {
		Key k = new Key().allocate(8);
		assert k.size() == 8;
		assert k.longValue() == 0;
	}

	public void testCreateFromLong() {
		Key k = Key.createFrom(10L);
		assert k.size() == 8;
		assert k.getByteBuffer() != null;
		assert k.getByteBuffer().position() == 0;
		assert k.getByteBuffer().capacity() == k.size();
		assert k.getByteBuffer().limit() == k.size();
		assert k.getByteBuffer().order() == ByteOrder.nativeOrder();
		assert k.longValue() == 10;
	}

	@Test(expectedExceptions=IllegalArgumentException.class)
	public void testCreateFromEmptyByteArray() {
		Key.createFrom(new byte[0]);
	}

	@Test(expectedExceptions=IllegalArgumentException.class)
	public void testCreateFromLargeByteArray() {
		Key.createFrom(new byte[FusionIOAPI.FUSION_IO_MAX_KEY_SIZE+1]);
	}

	public void testCreateFromByteArray() {
		byte[] data = new byte[] { 0x66, 0x6F, 0x6F, 0x00 };
		Key k = Key.createFrom(data);
		assert k.size() == 4;
		assert k.getByteBuffer() != null;
		assert k.getByteBuffer().position() == 0;
		assert k.getByteBuffer().capacity() == k.size();
		assert k.getByteBuffer().limit() == k.size();
		assert k.getByteBuffer().order() == ByteOrder.nativeOrder();

		ByteBuffer b = ByteBuffer.wrap(data);
		b.order(ByteOrder.nativeOrder());
		assert k.getByteBuffer().equals(b);
		assert k.getByteBuffer().get(0) == (byte)0x66;
	}
}
