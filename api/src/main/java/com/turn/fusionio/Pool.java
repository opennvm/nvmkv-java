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

import java.util.Iterator;
import java.util.Map;

/**
 * Represents a FusionIO-based key/value store pool.
 *
 * <p>
 * This class maps to the <tt>fio_kv_pool_t</tt> structure in the helper
 * library.
 * </p>
 *
 * @author mpetazzoni
 */
public class Pool implements Iterable<Map.Entry<Key, Value>> {

	/**
	 * Maximum batch size.
	 *
	 * <p>
	 * Maximum number of elements per batch operation.
	 * </p>
	 */
	public static final int FUSION_IO_MAX_BATCH_SIZE = 16;

	/** The FusionIO store this pool is a part of. */
	public final Store store;

	/** The pool ID. */
	public final int id;

	/** The pool tag. */
	public final String tag;

	Pool(Store store, int id, String tag) {
		this.store = store;
		this.id = id;
		this.tag = tag;
	}

	/**
	 * Retrieve a value from the key/value store.
	 *
	 * <p>
	 * It is the responsibility of the caller to manage the memory used by the
	 * {@link Value} object that is returned, in particular calling {@link
	 * Value#free()} to free the sector-aligned memory allocated on the native
	 * side by {@link Value#allocate(int)} or {@link Value#get(int)}.
	 * </p>
	 *
	 * @param key The key of the pair to retrieve.
	 * @return Returns an allocated {@link Value} containing the requested data.
	 * @throws FusionIOException If an error occurred while reading the
	 *	key/value pair from the store.
	 */
	public Value get(Key key) throws FusionIOException {
		if (!this.store.isOpened()) {
			throw new IllegalStateException("Key/value store is not opened!");
		}

		Value value = Value.get(
			FusionIOAPI.fio_kv_get_value_len(this, key));
		if (FusionIOAPI.fio_kv_get(this, key, value) < 0) {
			throw new FusionIOException("Error reading key/value pair!");
		}

		value.getByteBuffer().limit(value.size());
		return value;
	}

	/**
	 * Inserts or updates key/value pair into the key/value store.
	 *
	 * <p>
	 * It is the responsibility of the caller to manage the memory used by the
	 * {@link Value} object, in particular calling {@link Value#free()} to free
	 * the sector-aligned memory allocated on the native side by {@link
	 * Value#allocate(int)} or {@link Value#get(int)}.
	 * </p>
	 *
	 * <p>
	 * Note that if a pair already exists for the same key, it will be
	 * overwritten.
	 * </p>
	 *
	 * @param key The key.
	 * @param value The value to write for that key.
	 * @throws FusionIOException If an error occurred while writing this
	 *	key/value pair to the store.
	 */
	public void put(Key key, Value value) throws FusionIOException {
		if (!this.store.isOpened()) {
			throw new IllegalStateException("Key/value store is not opened!");
		}

		if (FusionIOAPI.fio_kv_put(this, key, value) != value.size()) {
			throw new FusionIOException("Error writing key/value pair!");
		}
	}

	/**
	 * Inserts or updates a set of key/value pairs into the key/value store in
	 * one batch operation.
	 *
	 * <p>
	 * It is the responsibility of the caller to manage the memory used by the
	 * {@link Value} object, in particular calling {@link Value#free()} to free
	 * the sector-aligned memory allocated on the native side by {@link
	 * Value#allocate(int)} or {@link Value#get(int)}.
	 * </p>
	 *
	 * <p>
	 * Note that if a pair already exists for the same key, it will be
	 * overwritten.
	 * </p>
	 *
	 * @param keys The array of keys.
	 * @param values An array of corresponding values.
	 * @throws FusionIOException If an error occurred while writing this
	 *	key/value pairs batch.
	 */
	public void put(Key[] keys, Value[] values) throws FusionIOException {
		if (!this.store.isOpened()) {
			throw new IllegalStateException("Key/value store is not opened!");
		}

		if (keys.length < 1 || keys.length > FUSION_IO_MAX_BATCH_SIZE ||
			keys.length != values.length) {
			throw new IllegalArgumentException("Invalid batch size!");
		}

		if (!FusionIOAPI.fio_kv_batch_put(this, keys, values)) {
			throw new FusionIOException("Error writing key/value pair batch!");
		}
	}

	/**
	 * Tells whether a given key exists in the key/value store.
	 *
	 * @param key The key as a {@link Key} object.
	 * @return Returns <em>true</em> if a mapping exists for the given key,
	 * <em>false</em> otherwise.
	 */
	public boolean exists(Key key) throws FusionIOException {
		return this.exists(key, null);
	}

	/**
	 * Tells whether a given key exists in the key/value store.
	 *
	 * @param key The key as a {@link Key} object.
	 * @param info An optional {@link KeyValueInfo} object to be filled with
	 *	information about the key/value pair, if it exists.
	 * @return Returns <em>true</em> if a mapping exists for the given key,
	 * <em>false</em> otherwise.
	 */
	public boolean exists(Key key, KeyValueInfo info) throws FusionIOException {
		if (!this.store.isOpened()) {
			throw new IllegalStateException("Key/value store is not opened!");
		}

		int ret = FusionIOAPI.fio_kv_exists(this, key, info);
		if (ret < 0) {
			throw new FusionIOException(
				"Error while verifying key/value pair presence!");
		}

		return ret == 1;
	}

	/**
	 * Remove a key/value pair.
	 *
	 * @param key The key of the pair to remove.
	 * @throws FusionIOException If the pair could not be removed because of a
	 *	low-level API error.
	 */
	public void delete(Key key) throws FusionIOException {
		if (!this.store.isOpened()) {
			throw new IllegalStateException("Key/value store is not opened!");
		}

		if (!FusionIOAPI.fio_kv_delete(this, key)) {
			throw new FusionIOException("Error removing key/value pair!");
		}
	}

	/**
	 * Retrieve an iterator on this key/value store.
	 *
	 * @return Returns a new iterator that returns key/value pairs from this store.
	 */
	@Override
	public Iterator<Map.Entry<Key, Value>> iterator() {
		if (!this.store.isOpened()) {
			throw new IllegalStateException("Key/value store is not opened!");
		}

		try {
			return new FusionIOStoreIterator(this);
		} catch (FusionIOException fioe) {
			return null;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || ! (o instanceof Pool)) {
			return false;
		}

		Pool other = (Pool) o;
		return this.store.equals(other.store) && this.id == other.id;
	}

	@Override
	public String toString() {
		return String.format("fusionio-pool(%s:%d)",
			this.store, this.id);
	}
}
