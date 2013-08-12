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

import com.turn.fusionio.FusionIOAPI.ExpiryMode;

import java.io.Closeable;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

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
public class Store implements Closeable {

	/** The path to the FusionIO device or directFS file. */
	public final String path;

	/** The file descriptor bound to the FusionIO device. */
	public final int fd;

	/** The KV store ID as understood by FusionIO's API. */
	public final long kv;

	private volatile boolean opened;
	private Map<String, Pool> pools;

	Store(String path) {
		this.path = path;
		this.fd = 0;
		this.kv = 0;

		this.opened = false;
		this.pools = this.getPools();
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
			if (!FusionIOAPI.fio_kv_open(this)) {
				throw new FusionIOException(
					"Could not open file or device for key/value API access!",
					FusionIOAPI.fio_kv_get_last_error());
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
			FusionIOAPI.fio_kv_close(this);
		}

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

		if (!FusionIOAPI.fio_kv_destroy(this)) {
			throw new FusionIOException(
				"Error while destroying the key/value store!",
				FusionIOAPI.fio_kv_get_last_error());
		}

		this.close();
	}

	/**
	 * Get an existing pool by its tag name or create a new one.
	 *
	 * <p>
	 * If a pool with the given tag name already exists no new pool is created
	 * and the existing one will returned instead.
	 * </p>
	 *
	 * @param tag The pool tag.
	 * @return Returns the {@link Pool} object that gives access to the
	 *	requested pool.
	 * @throws FusionIOException If the pool could not be created because of an
	 *	internal error.
	 */
	public synchronized Pool getOrCreatePool(String tag)
			throws FusionIOException {
		this.open();

		Pool pool = this.pools.get(tag);
		if (pool != null) {
			return pool;
		}

		pool = FusionIOAPI.fio_kv_create_pool(this, tag);
		if (pool != null) {
			this.pools.put(tag, pool);
			return pool;
		}

		throw new FusionIOException("Error while creating a new pool!",
			FusionIOAPI.fio_kv_get_last_error());
	}

	/**
	 * Get a pool by its ID.
	 *
	 * <p>
	 * You should not normally not have to use this, especially once pool
	 * discovery and pool tags are fully functional. But in the meantime, this
	 * provides a shortcut to accessing a pre-existing pool by its ID.
	 * </p>
	 *
	 * @param id The pool ID.
	 * @return The {@link Pool} object that gives access to the requested pool.
	 */
	public Pool getPool(int id) {
		return new Pool(this, id, null);
	}

	/**
	 * Retrieves information about all pools on this FusionIO store.
	 *
	 * @return Returns a new map of pool tags to their corresponding {@link
	 *	Pool} object.
	 */
	private Map<String, Pool> getPools() {
		return new HashMap<String, Pool>();
	}

	/**
	 * Returns a {@link StoreInfo} object containing metadata about this
	 * FusionIO store.
	 *
	 * <p>
	 * Note that this operation is synchronous and could take several minutes
	 * to return.
	 * </p>
	 *
	 * @return A populated {@link StoreInfo} object with metadata and
	 * information about this key/value store.
	 */
	public StoreInfo getInfo() throws FusionIOException {
		this.open();
		return FusionIOAPI.fio_kv_get_store_info(this);
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || ! (o instanceof Store)) {
			return false;
		}

		Store other = (Store) o;
		return this.path.equals(other.path);
	}

	@Override
	public String toString() {
		return String.format("fusionio-store(fd=%d,kv=%d)",
			this.fd, this.kv);
	}
}
