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
	private int length = 0;

	/** The key bytes themselves. */
	private ByteBuffer bytes = null;

	/**
	 * Return the key's size, in bytes.
	 *
	 * @return The key size, in bytes.
	 */
	public int size() {
		return this.length;
	}

	/**
	 * Return this key's data as a {@link ByteBuffer}.
	 *
	 * @return Returns a {@link ByteBuffer} object mapping to the native
	 *	memory allocated for this value's data.
	 */
	public ByteBuffer getByteBuffer() {
		return this.bytes;
	}

	/**
	 * Allocate memory for this {@link Key}.
	 *
	 * @param size The length, in bytes, of the key.
	 * @return Returns the {@link Key} object, for chaining.
	 * @throws IllegalArgumentException If the requested size is not between 1
	 *	and 128 bytes.
	 */
	public Key allocate(int size) {
		if (size <= 0 || size > FusionIOAPI.FUSION_IO_MAX_KEY_SIZE) {
			throw new IllegalArgumentException(
				"Invalid key size " + size + "!");
		}

		this.length = size;
		this.bytes = ByteBuffer.allocateDirect(this.length)
			.order(ByteOrder.nativeOrder());
		return this;
	}

	/**
	 * Get this {@link Key} as a long.
	 *
	 * @return The value held by this key, interpreted as a long.
	 */
	public long longValue() {
		if (this.length != 8) {
			throw new IllegalStateException("Key is not a long (" +
				this.bytes + ") !");
		}

		return this.bytes.getLong(0);
	}

	/**
	 * Initiliazes and allocates a new Key of the given size.
	 *
	 * @param size The key size, in bytes.
	 */
	public static Key get(int size) {
		return new Key().allocate(size);
	}

	/**
	 * Create a new {@link Key} from the given long ID.
	 *
	 * @param uid The key ID, as a long.
	 * @return Returns the constructed {@link Key} object for that UID value.
	 */
	public static Key createFrom(long uid) {
		Key k = Key.get(8);
		k.bytes.putLong(uid);
		k.bytes.rewind();
		return k;
	}

	/**
	 * Create a key from the given byte array.
	 *
	 * @param data The key's contents.
	 * @return Returns the newly created {@link Key}, with the data copied into
	 *	it.
	 * @throws IllegalArgumentException If the data size is not between 1 and
	 *	128 bytes.
	 */
	public static Key createFrom(byte[] data) {
		if (data.length <= 0 || data.length > FusionIOAPI.FUSION_IO_MAX_KEY_SIZE) {
			throw new IllegalArgumentException(
				"Invalid key size " + data.length + "!");
		}

		Key k = Key.get(data.length);
		k.bytes.put(data);
		k.bytes.rewind();
		return k;
	}
}
