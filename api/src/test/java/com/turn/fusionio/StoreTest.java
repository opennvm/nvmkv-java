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

import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for FusionIO stores.
 *
 * @author mpetazzoni
 */
@Test
public class StoreTest {

	private static final String DEVICE_NAME = "/dev/fioa";
	protected static final ExpiryMode TESTING_EXPIRY_MODE = ExpiryMode.GLOBAL_EXPIRY;
	protected static final int TESTING_EXPIRY_SECONDS = 2;
	private static final int POOL_DELETION_TIMEOUT_MS = 5000;

	private Store store = null;

	@BeforeMethod
	public void setUp(Method m) throws FusionIOException, InterruptedException {
		System.out.printf("%s.%s():\n", m.getDeclaringClass().getName(), m.getName());
		this.store = FusionIOAPI.get(DEVICE_NAME, 0,
			TESTING_EXPIRY_MODE, TESTING_EXPIRY_SECONDS);
		// Need to wait a bit for the device permissions to be set before
		// opening it.
		Thread.sleep(150);

		this.store.open();
		assert this.store.isOpened()
			: "FusionIO API was not opened as expected";
		assert this.store.getInfo().getExpiryMode() == ExpiryMode.GLOBAL_EXPIRY
			: "Expiry mode is not what expected. " +
				"Did you format the device before running the tests?";
		this.store.deleteAllPools();
		assert this.waitForPoolDeletion();
	}

	@AfterMethod
	public void tearDown(ITestResult tr)
			throws FusionIOException, InterruptedException {
		this.store.open();
		this.store.deleteAllPools();
		this.store.close();
		System.out.printf("%s(): done (%d)\n", tr.getName(), tr.getStatus());
	}

	private boolean waitForPoolDeletion() throws FusionIOException {
		long start = System.currentTimeMillis();

		while (System.currentTimeMillis() < start + POOL_DELETION_TIMEOUT_MS) {
			if (this.store.getInfo().getNumPools() == 0) {
				return true;
			}
		}

		return false;
	}

	public void testOpenClose() throws FusionIOException {
		assert this.store.fd != 0;
		assert this.store.kv != 0;
		this.store.close();
		assert !this.store.isOpened()
			: "FusionIO API should be closed now";
		assert this.store.fd == 0;
		assert this.store.kv == 0;
	}

	public void testDoubleOpen() throws FusionIOException {
		int fd = this.store.fd;
		long kv = this.store.kv;
		this.store.open();
		assert this.store.isOpened()
			: "FusionIO should have remained open";
		assert this.store.fd == fd;
		assert this.store.kv == kv;
	}

	public void testDoubleClose() throws FusionIOException {
		this.store.close();
		assert !this.store.isOpened()
			: "FusionIO API should be closed now";
		assert this.store.fd == 0;
		assert this.store.kv == 0;
		this.store.close();
		assert !this.store.isOpened()
			: "FusionIO API should have remained closed";
		assert this.store.fd == 0;
		assert this.store.kv == 0;
	}

	public void testGetInfo() throws FusionIOException {
		StoreInfo info = this.store.getInfo();
		assert info != null;
		assert info.getExpiryMode() == TESTING_EXPIRY_MODE
			: String.format("Invalid expiry mode %s, expected %s!",
				info.getExpiryMode(), TESTING_EXPIRY_MODE);
		assert info.getNumPools() == 0;
		assert info.getMaxPools() == Store.FUSION_IO_MAX_POOLS
			: info.getMaxPools();

		this.store.getOrCreatePool("foo");
		info = this.store.getInfo();
		assert info.getNumPools() == 1;
	}

	@Test(expectedExceptions=IllegalStateException.class)
	public void testGetInfoClosedStore() throws FusionIOException {
		this.store.close();
		this.store.getInfo();
	}

	// Very slow test
	@Test(enabled=false)
	public void testDeleteAll() throws FusionIOException {
		Key key = Key.createFrom(1L);
		Value value = Value.get(2);
		value.getByteBuffer().put(new byte[] {0x68, 0x00});
		value.getByteBuffer().rewind();

		Pool p = this.store.getPool(Pool.FUSION_IO_DEFAULT_POOL_ID);
		p.put(key, value);
		value.free();

		assert p.exists(key);
		this.store.clear();
		assert !p.exists(key);
	}

	public void testCreatePool() throws FusionIOException {
		Pool p = this.store.getOrCreatePool("test");
		assert p != null;
		assert p.id > 0;
		assert "test".equals(p.tag);
		assert this.store.getInfo().getNumPools() == 1;
	}

	public void testGetExistingPool() throws FusionIOException {
		Pool p = this.store.getOrCreatePool("test");
		Pool p2 = this.store.getOrCreatePool("test");
		assert p.equals(p2);
		assert this.store.getInfo().getNumPools() == 1;
	}

	public void testDeletePool() throws FusionIOException {
		Pool p = this.store.getOrCreatePool("test");
		assert this.store.getInfo().getNumPools() == 1;
		this.store.deletePool(p);
		assert this.waitForPoolDeletion();
		assert this.store.getInfo().getNumPools() == 0;
	}

	public void testForceError() throws FusionIOException {
		this.store.getOrCreatePool("test");
		this.store.getOrCreatePool("test2");
		assert this.store.getInfo().getNumPools() == 2;
		this.store.deleteAllPools();
		this.store.close();
		this.store.open();
		this.store.deleteAllPools();
		this.store.close();
		this.store.open();
		this.store.getOrCreatePool("test");
	}

	public void testGetAllPools() throws FusionIOException {
		assert this.store.getAllPools().length == 0;
		Pool test = this.store.getOrCreatePool("test");
		Pool foo = this.store.getOrCreatePool("foo");
		Pool[] pools = this.store.getAllPools();
		assert pools.length == 2;
		assert test.equals(pools[0]);
		assert foo.equals(pools[1]);
	}
}
