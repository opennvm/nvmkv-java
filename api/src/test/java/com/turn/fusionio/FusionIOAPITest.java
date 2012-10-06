/**
 * Copyright (C) 2012 Turn, Inc.  All Rights Reserved.
 * Proprietary and confidential.
 */
package com.turn.fusionio;

import com.turn.fusionio.FusionIOAPI.FusionIOException;
import com.turn.fusionio.FusionIOAPI.Key;
import com.turn.fusionio.FusionIOAPI.Value;

import java.nio.ByteBuffer;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for the low-level FusionIO API wrapping.
 *
 * <p>
 * These tests are marked as broken by default because they need FusionIO
 * hardware installed in the machine that runs them.
 * </p>
 *
 * @author mpetazzoni
 */
@Test
public class FusionIOAPITest {

	private static final Key TEST_UID = Key.get(42L);;
	private static final byte[] TEST_DATA = new byte[] {
		// hello, world
		0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00
	};

	private FusionIOAPI api = null;

	@BeforeClass
	public void setUp() throws FusionIOException {
		this.api = new FusionIOAPI("/dev/fct0", 0);
	}

	@BeforeMethod
	public void clean() throws FusionIOException {
		this.api.remove(TEST_UID);
	}

	@AfterClass
	public void tearDown() throws FusionIOException {
		this.api.close();
	}

	public void testOpenClose() throws FusionIOException {
		this.api.open();
		assert this.api.isOpened()
			: "FusionIO API was not opened as expected";
		this.api.close();
		assert !this.api.isOpened()
			: "FusionIO API should be closed now";
	}

	public void testPut() throws FusionIOException {
		Value value = Value.allocate(TEST_DATA.length);
		value.getByteBuffer().put(TEST_DATA);
		this.api.put(TEST_UID, value, 60);
		assert this.api.exists(TEST_UID)
			: "Key/value mapping should exist";
	}

	public void testGet() throws FusionIOException {
		Value value = Value.allocate(TEST_DATA.length);
		value.getByteBuffer().put(TEST_DATA);
		this.validatePutGetFlow(TEST_UID, value);
	}

	public void testRemove() throws FusionIOException {
		Value value = Value.allocate(TEST_DATA.length);
		value.getByteBuffer().put(TEST_DATA);
		this.api.put(TEST_UID, value, 60);
		assert this.api.exists(TEST_UID)
			: "Key/value mapping should exist";

		this.api.remove(TEST_UID);
		assert !this.api.exists(TEST_UID)
			: "Key/value mapping should have been removed";
	}

	public void testBigValue() throws FusionIOException {
		Value value = Value.allocate(1024);
		ByteBuffer data = value.getByteBuffer();
		while (data.remaining() > 0) {
			data.put((byte) 0x42);
		}

		this.validatePutGetFlow(TEST_UID, value);
	}

	private void validatePutGetFlow(Key key, Value value)
		throws FusionIOException {
		assert !this.api.exists(key)
			: "Key/value mapping should not exist";
		this.api.put(key, value, 60);

		Value readback = null;
		try {
			readback = Value.allocate(value.info.value_len);
			this.api.get(key, readback);
			assert value.getByteBuffer().equals(readback.getByteBuffer())
				: "Value data does not match";
		} finally {
			if (readback != null) {
				readback.free();
			}
		}
	}
}
