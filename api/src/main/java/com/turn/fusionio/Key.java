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
 * Represents a key for use in a FusionIO-based key/value store.
 *
 * <p>
 * This class maps to the <tt>fio_kv_key_t</tt> structure in the helper
 * library.
 * </p>
 *
 * @author mpetazzoni
 */
public class Key {

	/** The key length, in bytes. */
	private int length;

	/** The key bytes themselves. */
	private ByteBuffer bytes;

	/**
	 * Get this {@link Key} as a long.
	 *
	 * @return The value held by this key, interpreted as a long.
	 */
	public long longValue() {
		if (this.length != 8) {
			throw new IllegalStateException("Key is not a long!");
		}

		return this.bytes.getLong(0);
	}

	/**
	 * Set this {@link Key} to the given long ID.
	 *
	 * @param uid The key ID, as a long value.
	 * @return Returns the {@link Key} object, for chaining.
	 * @throws IllegalStateException If this key is not allocated, or not
	 *	allocated to host a long value (8 bytes).
	 */
	public Key set(long uid) {
		if (this.bytes == null || this.length != 8) {
			throw new IllegalStateException("Key is not allocated for long values!");
		}

		this.bytes.putLong(0, uid);
		return this;
	}

	/**
	 * Allocate memory for this {@link Key}.
	 *
	 * @param size The length, in bytes, of the key.
	 * @return Returns the {@link Key} object, for chaining.
	 */
	public Key allocate(int size) {
		this.length = size;
		this.bytes = ByteBuffer.allocateDirect(this.length)
			.order(ByteOrder.nativeOrder());
		return this;
	}

	/**
	 * Create a new {@link Key} for the given long ID.
	 *
	 * @param uid The key ID, as a long.
	 * @return Returns the constructed {@link Key} object for that UID value.
	 */
	public static Key get(long uid) {
		return new Key().allocate(8).set(uid);
	}
}
