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

import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.Structure;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Low-level access API to a FusionIO device key/value store functionality.
 *
 * <p>
 * This class provides a convenient Java facade to FusionIO's key/value store
 * API. It makes uses of an intermediary helper library which is then mapped to
 * Java via JNA.
 * </p>
 *
 * @author mpetazzoni
 */
public class FusionIOAPI {

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
	 * The name of the helper library name.
	 *
	 * <p>
	 * This shared library needs to be in the library path of the executing
	 * JVM, either by using the <tt>LD_LIBRARY_PATH</tt> environment variable
	 * or by setting <tt>-Djava.library.path</tt>.
	 * </p>
	 */
	private static final String LIBRARY_NAME = "fio_kv_helper";

	/**
	 * The JNA instance of this library, if enabled.
	 */
	protected static HelperLibrary instance = new NativeHelperLibrary();

	private final String device;
	private final int poolId;
	private Store store;

	/**
	 * Instantiate a new FusionIO access API facade.
	 *
	 * @param device The device filename of the FusionIO device.
	 * @param poolId The ID of the key/value pool to use on the device.
	 * @throws UnsupportedOperationException If FusionIO storage is disabled.
	 */
	public FusionIOAPI(String device, int poolId) {
		if (instance == null) {
			throw new UnsupportedOperationException("FusionIO storage is not enabled.");
		}

		this.device = device;
		this.poolId = poolId;
		this.store = null;
	}

	/**
	 * Open the FusionIO key/value store backing this {@link FusionIOAPI}.
	 *
	 * @throws FusionIOException If the FusionIO device cannot be opened.
	 */
	public void open() throws FusionIOException {
		if (this.isOpened()) {
			return;
		}

		synchronized (this) {
			this.store = instance.fio_kv_open(this.device, this.poolId);
			if (this.store == null) {
				throw new FusionIOException("Could not open device for key/value API access!");
			}
		}
	}

	/**
	 * Tells whether the FusionIO device was opened for access by this API or
	 * not.
	 */
	public boolean isOpened() {
		return this.store != null && this.store.kv > 0;
	}

	/**
	 * Close the key/value store backing this {@link FusionIOAPI}.
	 *
	 * <p>
	 * If the key/value store is already closed, this method does nothing.
	 * </p>
	 */
	public synchronized void close() throws FusionIOException {
		if (this.isOpened()) {
			instance.fio_kv_close(this.store);
		}

		this.store = null;
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
	 * @param exiry The desired expiry of this key/value pair, in seconds.
	 * @throws FusionIOException If an error occurred while writing this
	 * key/value pair to the store. Would that occur, any existing key/value
	 * pair for that key is removed (even if one existed before) to make sure
	 * we don't leave partial values or invalid data in the key/value store.
	 */
	public void put(Key key, Value value, int expiry) throws FusionIOException {
		this.open();

		value.info.expiry = expiry;
		if (instance.fio_kv_put(this.store, key, value) != value.info.value_len) {
			this.remove(key);
			throw new FusionIOException("Error writing key/value pair!");
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
		this.open();
		return instance.fio_kv_exists(this.store, key);
	}

	/**
	 * Retrieve a value from the key/value store.
	 *
	 * @param key The key of the pair to retrieve.
	 * @param value The value to read into. It must be obtained with {@link
	 * Value.allocate()} so that the memory holding the pair data is
	 *	sector-aligned.
	 * @throws FusionIOException If an error occurred while retrieving the data.
	 */
	public void get(Key key, Value value) throws FusionIOException {
		this.open();

		if (!this.exists(key)) {
			return;
		}

		if (instance.fio_kv_get(this.store, key, value) < 0) {
			throw new FusionIOException("Error reading key/value pair!");
		}
	}

	/**
	 * Remove a key/value pair.
	 *
	 * @param key The key of the pair to remove.
	 * @throws FusionIOException If the pair could not be removed because of a
	 * low-level API error.
	 */
	public void remove(Key key) throws FusionIOException {
		this.open();

		if (!instance.fio_kv_delete(this.store, key)) {
			throw new FusionIOException("Error removing key/value pair!");
		}
	}

	@Override
	public String toString() {
		return String.format("fusionio-api(device=%s,pool=%d,opened=%s)",
			this.device, this.poolId,
			(this.isOpened() ? this.store.toString() : "no"));
	}

	/**
	 * JNA library mapping for the helper library.
	 *
	 * <p>
	 * This is a JNA-enabled wrapping of the libfio_kv_helper library built
	 * around FusionIO's key/value store library. It provides an easier to use
	 * API better suited for use in the Turn platform.
	 * </p>
	 *
	 * @author mpetazzoni
	 * @see src/cpp/fio_kv_helper/fio_kv_helper.h
	 */
	private interface HelperLibrary extends Library {
		public Store fio_kv_open(String device, int pool_id);
		public void fio_kv_close(Store store);
		public Pointer fio_kv_alloc(int length);
		public void fio_kv_free_value(Value value);
		public int fio_kv_get(Store store, Key key, Value value);
		public int fio_kv_put(Store store, Key key, Value value);
		public boolean fio_kv_exists(Store store, Key key);
		public boolean fio_kv_delete(Store store, Key key);
	};

	/**
	 * JNA direct-mapping implementation of the {@link HelperLibrary}.
	 *
	 * <p>
	 * This implementation of the {@link HelperLibrary} for low-level FusionIO
	 * key/value store API access uses JNA's direct-mapping feature through
	 * native methods instead of the slower, runtime-mapping interfacing.
	 * </p>
	 *
	 * @author mpetazzoni
	 */
	private static class NativeHelperLibrary implements HelperLibrary {

		static {
			try {
				Native.register(LIBRARY_NAME);
			} catch (Exception e) {
				System.err.println("Could not initialize FusionIO binding: " +
					e.getMessage());
			}
		};

		public native Store fio_kv_open(String device, int pool_id);
		public native void fio_kv_close(Store store);
		public native Pointer fio_kv_alloc(int length);
		public native void fio_kv_free_value(Value value);
		public native int fio_kv_put(Store store, Key key, Value value);
		public native int fio_kv_get(Store store, Key key, Value value);
		public native boolean fio_kv_exists(Store store, Key key);
		public native boolean fio_kv_delete(Store store, Key key);
	};

	/**
	 * Represents a FusionIO-based key/value store.
	 *
	 * <p>
	 * This class maps to the <tt>fio_kv_store_t</tt> structure in the helper
	 * library.
	 * </p>
	 *
	 * @author mpetazzoni
	 */
	public static class Store extends Structure {

		private static final String[] FIELD_ORDER = new String[] {"fd", "kv", "pool"};

		/** The file descriptor bound to the FusionIO device. */
		public int fd;

		/** The KV store ID as understood by FusionIO's API. */
		public long kv;

		/** The pool ID in the KV store. */
		public int pool;

		public Store() {
			this.setFieldOrder(FIELD_ORDER);
		}

		@Override
		public String toString() {
			return String.format("fusionio-store(fd=%d,kv=%d,pool=%d)",
				this.fd, this.kv, this.pool);
		}
	};


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
	public static class Key extends Structure {

		private static final String[] FIELD_ORDER = new String[] {"length", "bytes"};

		/** The key length, in bytes. */
		public int length;

		/** The key bytes themselves. */
		public Pointer bytes;

		public Key() {
			this.setFieldOrder(FIELD_ORDER);
		}

		public long longValue() {
			if (this.length != 8) {
				throw new IllegalStateException("Key is not a long!");
			}

			return this.bytes.getLong(0);
		}

		/**
		 * Create a new {@link Key} object from a long value.
		 *
		 * @param uid The key ID, as a long.
		 * @return Returns a constructed {@link Key} object for that UID value.
		 */
		public static Key get(long uid) {
			Key k = new Key();
			k.length = 8;
			k.bytes = new Memory(k.length);
			k.bytes.setLong(0, uid);
			return k;
		}
	};


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
	public static class Value extends Structure {

		private static final String[] FIELD_ORDER = new String[] {"data", "info"};

		/** A {@link Pointer} to the memory holding the value. */
		public Pointer data;

		/**
		 * A reference to the {@link KeyValueInfo} structure holding
		 * information about the key/value mapping to insert or that was just
		 * retrieved.
		 */
		public KeyValueInfo info;

		public Value() {
			this.setFieldOrder(FIELD_ORDER);
		}

		/**
		 * Return this value's data as a {@link ByteBuffer}.
		 *
		 * @return Returns a {@link ByteBuffer} object mapping to the native
		 * memory allocated for this value's data.
		 */
		public ByteBuffer getByteBuffer() {
			return this.data != null && this.info != null
				? this.data.getByteBuffer(0, this.info.value_len)
				: null;
		}

		/**
		 * Free the native memory used by this value's data.
		 */
		public void free() {
			instance.fio_kv_free_value(this);
		}

		/**
		 * Creates a new {@link Value} object with allocated memory to hold its
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
		 * memory.
		 */
		public static Value allocate(int size) {
			Value v = new Value();
			v.data = instance.fio_kv_alloc(size);
			v.info = new KeyValueInfo();
			v.info.value_len = size;
			return v;
		}
	};


	/**
	 * Represents the info structure of a key/value mapping.
	 *
	 * <p>
	 * This structure mapping for <tt>kv_key_info_t</tt> comes directly from
	 * FusionIO's KV API library.
	 * </p>
	 *
	 * @author mpetazzoni
	 */
	public static class KeyValueInfo extends Structure
			implements Structure.ByReference {

		private static final String[] FIELD_ORDER =
			new String[] {"pool_id", "key_len", "value_len", "expiry", "gen_count"};

		/** The pool ID the mapping was read from. */
		public int pool_id;

		/** The length, in bytes, of the key. */
		public int key_len;

		/** The length, in bytes, of the value. */
		public int value_len;

		/** The expiry, in seconds, configured for this mapping. */
		public int expiry;

		/** The generation count for this mapping (this is not an
		 * API-controlled value but is free to the user of the API). */
		public int gen_count;

		public KeyValueInfo() {
			this.setFieldOrder(FIELD_ORDER);
		}
	};


	/**
	 * Exception class for low-level FusionIO API errors.
	 *
	 * @author mpetazzoni
	 */
	public static class FusionIOException extends IOException {

		public static final long serialVersionUID = 1L;

		private final int errno;

		public FusionIOException(String message) {
			super(message);
			this.errno = 0;
		}

		public FusionIOException(String message, Throwable cause) {
			super(message, cause);
			this.errno = 0;
		}

		public FusionIOException(String message, int errno) {
			super(message);
			this.errno = errno;
		}

		public FusionIOException(String message, Throwable cause, int errno) {
			super(message, cause);
			this.errno = errno;
		}

		public String getMessage() {
			if (this.errno == 0) {
				return super.getMessage();
			}

			return String.format("%s [%d]", super.getMessage(), this.errno);
		}
	};
}
