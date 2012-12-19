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

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for FusionIO stores.
 *
 * @author mpetazzoni
 */
@Test
public class StoreTest {

	private static final String DEVICE_NAME = "/dev/fioa";

	private Store store = null;

	@BeforeClass
	public void setUp() throws FusionIOException {
		this.store = new Store(DEVICE_NAME);
		this.store.destroy();
		assert !this.store.isOpened()
			: "The key/value store should be closed after destroy!";

		// Need to wait a bit for permission reset on device file.
		try { Thread.sleep(250); } catch (InterruptedException ie) { }
	}

	@AfterClass
	public void tearDown() throws FusionIOException {
		this.store.destroy();
		this.store.close();
	}

	public void testOpenClose() throws FusionIOException {
		this.store.open();
		assert this.store.isOpened()
			: "FusionIO API was not opened as expected";
		assert this.store.fd != 0;
		assert this.store.kv != 0;
		this.store.close();
		assert !this.store.isOpened()
			: "FusionIO API should be closed now";
		assert this.store.fd == 0;
		assert this.store.kv == 0;
	}

	public void testDoubleOpen() throws FusionIOException {
		this.store.open();
		assert this.store.isOpened()
			: "FusionIO API was not opened as expected";
		int fd = this.store.fd;
		long kv = this.store.kv;
		this.store.open();
		assert this.store.isOpened()
			: "FusionIO should have remained open";
		assert this.store.fd == fd;
		assert this.store.kv == kv;
	}

	public void testDoubleClose() throws FusionIOException {
		this.store.open();
		assert this.store.isOpened()
			: "FusionIO API was not opened as expected";
		this.store.close();
		assert !this.store.isOpened()
			: "FusionIO API should be closed now";
		this.store.close();
		assert !this.store.isOpened()
			: "FusionIO API should have remained closed";
		assert this.store.fd == 0;
		assert this.store.kv == 0;
	}
}
