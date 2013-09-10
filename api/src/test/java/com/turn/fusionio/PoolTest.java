/**
 * Copyright (C) 2012-2013 Turn Inc.  All Rights Reserved.
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
 * - Neither the name Turn Inc. nor the names of its contributors may be used
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

import com.turn.fusionio.FusionIOAPI.ExpiryMode;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for FusionIO key/value store pools.
 *
 * @author mpetazzoni
 */
@Test
public class PoolTest {

	private static final String DEVICE_NAME = "/dev/fioa";

	private static final int BATCH_SIZE = 10;
	private static final int BIG_VALUE_SIZE = 4096;
	private static final int EXPIRY_SECONDS = 2;

	private static final Key[] TEST_KEYS = new Key[BATCH_SIZE];
	private static final Value[] TEST_VALUES = new Value[BATCH_SIZE];
	private static final Value[] TEST_READBACK = new Value[BATCH_SIZE];

	// "hello, world"
	private static final byte[] TEST_DATA = new byte[] {
		0x68, 0x65, 0x6c, 0x6c, 0x6f,
		0x2c, 0x20,
		0x77, 0x6f, 0x72, 0x6c, 0x64,
		0x00,
	};

	private Store store = null;
	private Pool pool = null;

	@BeforeClass
	public void setUpClass() throws FusionIOException, InterruptedException {
		for (int i=0; i<BATCH_SIZE; i++) {
			TEST_KEYS[i] = Key.createFrom(i);
			TEST_VALUES[i] = Value.get(TEST_DATA.length);
			TEST_VALUES[i].getByteBuffer().put(TEST_DATA);
			TEST_VALUES[i].getByteBuffer().rewind();
			TEST_READBACK[i] = Value.get(TEST_DATA.length);
		}

		this.store = FusionIOAPI.get(DEVICE_NAME, 0,
			StoreTest.TESTING_EXPIRY_MODE,
			StoreTest.TESTING_EXPIRY_SECONDS);
		// Need to wait a bit for the device permissions to be set before
		// opening it.
		Thread.sleep(150);

		System.out.println("open()...");
		this.store.open();
		assert this.store.isOpened()
			: "FusionIO API was not opened as expected";
		System.out.println("getInfo()...");
		assert this.store.getInfo().getExpiryMode() == ExpiryMode.GLOBAL_EXPIRY
			: "Expiry mode is not what expected. " +
				"Did you format the device before running the tests?";
		System.out.println("getOrCreatePool()...");
		this.pool = this.store.getOrCreatePool("test");
		System.out.println("init complete");
	}

	@BeforeMethod
	public void setUp(Method m) throws FusionIOException {
		System.out.printf("%s.%s():\n", m.getDeclaringClass().getName(), m.getName());
		for (int i=0; i<BATCH_SIZE; i++) {
			this.pool.delete(TEST_KEYS[i]);
			TEST_READBACK[i]
				.free()
				.allocate(TEST_DATA.length);
		}
	}

	@AfterMethod
	public void after(ITestResult tr) {
		System.out.printf("%s(): done (%d)\n", tr.getName(), tr.getStatus());
		System.out.flush();
	}

	@AfterClass
	public void tearDown() throws FusionIOException {
		this.store.deleteAllPools();
		this.store.close();
	}

	public void testGet() throws FusionIOException {
		this.pool.put(TEST_KEYS[0], TEST_VALUES[0]);
		assert this.pool.exists(TEST_KEYS[0])
			: "Key/value mapping should exist";
		Value readback = this.pool.get(TEST_KEYS[0]);
		Assert.assertEquals(
			readback.getByteBuffer(),
			TEST_VALUES[0].getByteBuffer());
		readback.free();
	}

	public void testPut() throws FusionIOException {
		this.pool.put(TEST_KEYS[0], TEST_VALUES[0]);
		assert this.pool.exists(TEST_KEYS[0])
			: "Key/value mapping should exist";
	}

	public void testGetKeyValueInfo() throws FusionIOException {
		this.pool.put(TEST_KEYS[0], TEST_VALUES[0]);
		KeyValueInfo info = this.pool.getKeyValueInfo(TEST_KEYS[0]);
		assert info != null;
		assert info.pool_id == this.pool.id;
		assert info.key_len == TEST_KEYS[0].size();
		assert info.value_len == TEST_VALUES[0].size();
	}

	public void testExpiry() throws FusionIOException, InterruptedException {
		this.pool.put(TEST_KEYS[0], TEST_VALUES[0]);
		assert this.pool.exists(TEST_KEYS[0])
			: "Key/value mapping should exist";
		Thread.sleep(TimeUnit.SECONDS.toMillis(EXPIRY_SECONDS+1));
		assert !this.pool.exists(TEST_KEYS[0])
			: "Key/value mapping should no longer exist";
	}

	public void testRemove() throws FusionIOException {
		this.pool.put(TEST_KEYS[0], TEST_VALUES[0]);
		assert this.pool.exists(TEST_KEYS[0])
			: "Key/value mapping should exist";

		this.pool.delete(TEST_KEYS[0]);
		assert !this.pool.exists(TEST_KEYS[0])
			: "Key/value mapping should have been removed";
	}

	public void testBigValue() throws FusionIOException {
		Value value = Value.get(BIG_VALUE_SIZE);
		ByteBuffer data = value.getByteBuffer();
		while (data.remaining() > 0) {
			data.put((byte) 0x42);
		}
		data.rewind();

		this.pool.put(TEST_KEYS[0], value);
		assert this.pool.exists(TEST_KEYS[0])
			: "Key/value mapping should exist";
		Value readback = this.pool.get(TEST_KEYS[0]);
		Assert.assertEquals(readback.getByteBuffer(), data);
		readback.free();
	}

	public void testBatchPut() throws FusionIOException {
		this.pool.put(TEST_KEYS, TEST_VALUES);

		for (int i=0; i<BATCH_SIZE; i++) {
			Value readback = this.pool.get(TEST_KEYS[i]);
			Assert.assertEquals(
				readback.getByteBuffer(),
				TEST_VALUES[i].getByteBuffer(),
				"Value #" + i + "'s data does not match!");
			readback.free();
		}
	}

	public void testIteration() throws FusionIOException {
		this.pool.put(TEST_KEYS, TEST_VALUES);

		int count = 0;
		for (Map.Entry<Key, Value> pair : this.pool) {
			assert count < TEST_VALUES.length
				: "Iteration returned too many items!";
			Assert.assertEquals(
				pair.getValue().getByteBuffer(),
				TEST_VALUES[(int)pair.getKey().longValue()].getByteBuffer(),
				"Value data mismatch for key " + count + "!");
			count++;
		}
		assert count == TEST_VALUES.length;
	}

	public void testPoolPartitioning() throws FusionIOException {
		Pool a = this.store.getOrCreatePool("foo");
		assert a != null;
		assert a.id > 0;
		assert "foo".equals(a.tag);

		Pool b = this.store.getOrCreatePool("bar");
		assert b != null;
		assert b.id > 0;
		assert b.id != a.id;
		assert "bar".equals(b.tag);

		a.put(TEST_KEYS, TEST_VALUES);

		// For now creating an iterator on an empty pool fails by design, but
		// that's all we need to check.
		assert b.iterator() == null;
	}
}
