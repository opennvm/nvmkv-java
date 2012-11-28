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

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

/**
 * Low-level access API to a FusionIO device key/value store functionality.
 *
 * <p>
 * This class provides a convenient Java facade to FusionIO's key/value store
 * API. It makes uses of an intermediary helper library which is then mapped to
 * Java via JNI.
 * </p>
 *
 * @author mpetazzoni
 */
public class FusionIOAPI implements Closeable, Iterable<Map.Entry<Key, Value>> {

	/**
	 * Maximum key size supported by FusionIO.
	 *
	 * <p>
	 * The maximum key size is 128 bytes.
	 * </p>
	 */
	public static final int FUSION_IO_MAX_KEY_SIZE = 128;

	/**
	 * Maximum value size supported by FusionIO.
	 *
	 * <p>
	 * The maximum value size is 1MiB - 1KiB.
	 * </p>
	 */
	public static final int FUSION_IO_MAX_VALUE_SIZE = 1024 * 1023;

	/**
	 * Maximum batch size.
	 *
	 * <p>
	 * Maximum number of elements per batch operation.
	 * </p>
	 */
	public static final int FUSION_IO_MAX_BATCH_SIZE = 16;

	/**
	 * The name of the helper library name.
	 *
	 * <p>
	 * This shared library needs to be in the library path of the executing
	 * JVM, either by using the <tt>LD_LIBRARY_PATH</tt> environment variable
	 * or by setting <tt>-Djava.library.path</tt>.
	 * </p>
	 */
	private static final String LIBRARY_NAME = "fio_kv_helper";

	private final String path;
	private final int poolId;
	private Store store;

	private volatile boolean opened;

	/**
	 * Instantiate a new FusionIO access API facade.
	 *
	 * @param path The FusionIO device filename or directFS filenam path.
	 * @param poolId The ID of the key/value pool to use on the device.
	 * @throws UnsupportedOperationException If FusionIO storage is disabled.
	 */
	public FusionIOAPI(String path, int poolId) {
		this.path = path;
		this.poolId = poolId;
		this.store = null;
		this.opened = false;
	}

	/**
	 * Open the FusionIO key/value store backing this {@link FusionIOAPI}.
	 *
	 * @throws FusionIOException If the key/value store cannot be opened.
	 */
	public void open() throws FusionIOException {
		if (this.opened) {
			return;
		}

		synchronized (this) {
			this.store = HelperLibrary.fio_kv_open(this.path, this.poolId);
			if (this.store == null) {
				throw new FusionIOException(
					"Could not open file or device for key/value API access!",
					HelperLibrary.fio_kv_get_last_error());
			}

			this.opened = true;
		}
	}

	/**
	 * Tells whether the key/value store was opened for access by this API or
	 * not.
	 */
	public boolean isOpened() {
		return this.opened;
	}

	/**
	 * Close the key/value store backing this {@link FusionIOAPI}.
	 *
	 * <p>
	 * If the key/value store is already closed, this method does nothing.
	 * </p>
	 */
	@Override
	public synchronized void close() throws FusionIOException {
		if (this.opened) {
			HelperLibrary.fio_kv_close(this.store);
		}

		this.store = null;
		this.opened = false;
	}

	/**
	 * Destroy the key/value store backing this {@link FusionIOAPI}.
	 *
	 * <p>
	 * <b>Warning:</b> this method will destroy all data and all pools in this
	 * key/value store! Use with care!
	 * </p>
	 *
	 * The FusionIO store will be closed after the operation.
	 *
	 * @throws FusionIOException If an error occured while destroying the
	 *	key/value store.
	 */
	public synchronized void destroy() throws FusionIOException {
		this.open();

		// TODO(mpetazzoni): remove this check once nvm_kv_destroy()'s behavior
		// for directFS-based key/value stores has been defined.
		if (!this.path.startsWith("/dev")) {
			this.close();
			new File(this.path).delete();
			return;
		}

		if (!HelperLibrary.fio_kv_destroy(this.store)) {
			throw new FusionIOException(
				"Error while destroying the key/value store!",
				HelperLibrary.fio_kv_get_last_error());
		}

		this.close();
	}

	/**
	 * Retrieve a value from the key/value store.
	 *
	 * <p>
	 * It is the responsibility of the caller to manage the memory used by the
	 * {@link Value} object, in particular calling {@link Value.free()} to free
	 * the sector-aligned memory allocated on the native side by {@link
	 * Value.allocate()}.
	 * </p>
	 *
	 * @param key The key of the pair to retrieve.
	 * @param value The value to read into. It must be obtained with {@link
	 *	Value.allocate()} so that the memory holding the pair data is
	 *	sector-aligned.
	 * @throws FusionIOException If an error occurred while retrieving the data.
	 */
	public void get(Key key, Value value) throws FusionIOException {
		this.open();

		if (HelperLibrary.fio_kv_get(this.store, key, value) < 0) {
			throw new FusionIOException("Error reading key/value pair!");
		}
	}

	/**
	 * Retrieve a set of values from the key/value store.
	 *
	 * <p>
	 * It is the responsibility of the caller to manage the memory used by each
	 * {@link Value} object, in particular calling {@link Value.free()} to free
	 * the sector-aligned memory allocated on the native side by {@link
	 * Value.allocate()}.
	 * </p>
	 *
	 * @param keys The set of keys for the pairs to retrieve.
	 * @param value An array of allocated {@link Value} objects to read each
	 *	value into.
	 * @throws FusionIOException If an error occurred while retrieving the data.
	 */
	public void get(Key[] keys, Value[] values) throws FusionIOException {
		this.open();

		if (keys.length < 1 || keys.length > FUSION_IO_MAX_BATCH_SIZE ||
			keys.length != values.length) {
			throw new IllegalArgumentException("Invalid batch size!");
		}

		if (!HelperLibrary.fio_kv_batch_get(this.store, keys, values)) {
			throw new FusionIOException("Error reading key/value pair batch!");
		}
	}

	/**
	 * Inserts or updates key/value pair into the key/value store.
	 *
	 * <p>
	 * It is the responsibility of the caller to manage the memory used by the
	 * {@link Value} object, in particular calling {@link Value.free()} to free
	 * the sector-aligned memory allocated on the native side by {@link
	 * Value.allocate()}.
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
		this.open();

		if (HelperLibrary.fio_kv_put(this.store, key, value) != value.size()) {
			throw new FusionIOException("Error writing key/value pair!");
		}
	}

	/**
	 * Inserts or updates a set of key/value pairs into the key/value store in
	 * one batch operation.
	 *
	 * <p>
	 * It is the responsibility of the caller to manage the memory used by the
	 * {@link Value} object, in particular calling {@link Value.free()} to free
	 * the sector-aligned memory allocated on the native side by {@link
	 * Value.allocate()}.
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
		this.open();

		if (keys.length < 1 || keys.length > FUSION_IO_MAX_BATCH_SIZE ||
			keys.length != values.length) {
			throw new IllegalArgumentException("Invalid batch size!");
		}

		if (!HelperLibrary.fio_kv_batch_put(this.store, keys, values)) {
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
		this.open();

		int ret = HelperLibrary.fio_kv_exists(this.store, key, info);
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
	public void remove(Key key) throws FusionIOException {
		this.open();

		if (!HelperLibrary.fio_kv_delete(this.store, key)) {
			throw new FusionIOException("Error removing key/value pair!");
		}
	}

	/**
	 * Remove a set of key/value pairs in one batch operation.
	 *
	 * @param keys The keys of the pairs to remove.
	 * @throws FusionIOException If the batch operation was not successful.
	 */
	public void remove(Key[] keys) throws FusionIOException {
		this.open();

		if (keys.length < 1 || keys.length > FUSION_IO_MAX_BATCH_SIZE) {
			throw new IllegalArgumentException("Invalid batch size!");
		}

		if (!HelperLibrary.fio_kv_batch_delete(this.store, keys)) {
			throw new FusionIOException("Error removing key/value pair batch!");
		}
	}

	/**
	 * Retrieve an iterator on this key/value store.
	 *
	 * @return Returns a new iterator that returns key/value pairs from this store.
	 */
	@Override
	public Iterator<Map.Entry<Key, Value>> iterator() {
		try {
			this.open();
			return new FusionIOStoreIterator(this.store);
		} catch (FusionIOException fioe) {
			return null;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || ! (o instanceof FusionIOAPI)) {
			return false;
		}

		FusionIOAPI other = (FusionIOAPI) o;
		return this.path.equals(other.path) && this.poolId == other.poolId;
	}

	@Override
	public String toString() {
		return String.format("fusionio-api(%s:%d, %s)",
			this.path, this.poolId,
			(this.isOpened() ? this.store.toString() : "closed"));
	}

	/**
	 * JNI library mapping.
	 *
	 * @author mpetazzoni
	 */
	static class HelperLibrary {

		static {
			try {
				System.loadLibrary(LIBRARY_NAME);
				fio_kv_init_jni_cache();
			} catch (Exception e) {
				System.err.println("Could not initialize FusionIO binding!");
				e.printStackTrace(System.err);
			}
		};

		public static native void fio_kv_init_jni_cache();

		public static native Store fio_kv_open(String path, int pool_id);
		public static native void fio_kv_close(Store store);
		public static native boolean fio_kv_destroy(Store store);

		public static native ByteBuffer fio_kv_alloc(int length);
		public static native void fio_kv_free_value(Value value);

		public static native int fio_kv_get(Store store, Key key, Value value);
		public static native int fio_kv_put(Store store, Key key, Value value);
		public static native int fio_kv_exists(Store store, Key key, KeyValueInfo info);
		public static native boolean fio_kv_delete(Store store, Key key);

		public static native boolean fio_kv_batch_get(Store store, Key[] keys, Value[] values);
		public static native boolean fio_kv_batch_put(Store store, Key[] keys, Value[] values);
		public static native boolean fio_kv_batch_delete(Store store, Key[] keys);

		public static native int fio_kv_iterator(Store store);
		public static native boolean fio_kv_next(Store store, int iterator);
		public static native boolean fio_kv_get_current(Store store, int iterator, Key key, Value value);

		public static native int fio_kv_get_last_error();
	};
}
