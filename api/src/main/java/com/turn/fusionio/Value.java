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

/**
 * Represents a value for use in a FusionIO-based key/value store.
 *
 * <p>
 * This class maps to the <tt>fio_kv_value_t</tt> structure in the helper
 * library.
 * </p>
 *
 * @author mpetazzoni
 */
public class Value {

	/** A {@link ByteBuffer} holding the value's contents. */
	private ByteBuffer data = null;

	/**
	 * A reference to the {@link KeyValueInfo} structure holding
	 * information about the key/value mapping to insert or that was just
	 * retrieved.
	 */
	private KeyValueInfo info = null;

	/**
	 * Return this value's data as a {@link ByteBuffer}.
	 *
	 * @return Returns a {@link ByteBuffer} object mapping to the native
	 *	memory allocated for this value's data.
	 */
	public ByteBuffer getByteBuffer() {
		return this.data;
	}

	/**
	 * Return the value's size, in bytes.
	 *
	 * @return The value size, in bytes.
	 */
	public int size() {
		return this.info.value_len;
	}

	/**
	 * Allocate sector-aligned memory to hold this value's content.
	 *
	 * @param size The memory size, in bytes.
	 * @return Returns the {@link Value} object, for chaining.
	 */
	public Value allocate(int size) {
		if (this.data != null) {
			throw new IllegalStateException("Value is already allocated!");
		}

		this.data = FusionIOAPI.HelperLibrary.fio_kv_alloc(size)
			.order(ByteOrder.nativeOrder());
		this.info = new KeyValueInfo();
		this.info.value_len = size;
		return this;
	}

	/**
	 * Set the expiration delay for this value, in seconds.
	 *
	 * @param expiry The expiry delay in seconds.
	 * @return Returns thi {@link Value} object, for chaining.
	 */
	public Value setExpiry(int expiry) {
		if (this.info == null) {
			throw new IllegalStateException("Value is not allocated!");
		}

		this.info.expiry = expiry;
		return this;
	}

	/**
	 * Free the native memory used by this value's data.
	 *
	 * @return Returns the {@link Value} object, for chaining.
	 */
	public Value free() {
		FusionIOAPI.HelperLibrary.fio_kv_free_value(this);
		this.info = null;
		return this;
	}

	@Override
	public String toString() {
		byte[] data = new byte[this.info.value_len];
		this.data.mark();
		this.data.get(data);
		this.data.reset();
		return new String(data);
	}

	/**
	 * Initializes this {@link Value} object with allocated memory to hold its
	 * data.
	 *
	 * <p>
	 * This method creates a new value for use with this API and requests
	 * sector-aligned memory from the native side to hold the value's data.
	 * After use, the {@link free()} method must be called on the {@link
	 * Value} object to release the allocated memory.
	 * </p>
	 *
	 * @param size The size, in bytes, to allocate for.
	 * @return Returns a ready-to-use {@link Value} object with allocated
	 *	memory.
	 */
	public static Value get(int size) {
		return new Value().allocate(size);
	}
}
