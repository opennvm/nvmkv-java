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

	Store(String path) {
		this.path = path;
		this.fd = 0;
		this.kv = 0;

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
	 * Get a pool by its ID.
	 *
	 * <p>
	 * Note that no checks are performed to validate the existence of the pool.
	 * </p>
	 *
	 * @param poolId The ID of the pool to retrieve.
	 * @return Returns the corresponding key/value store pool.
	 */
	public Pool getPool(int poolId) {
		return new Pool(this, poolId, null);
	}

	/**
	 * Create a new pool in this key/value store.
	 *
	 * @param tag The pool tag.
	 * @throws FusionIOException If the pool could not be created because of an
	 *	internal error.
	 */
	public synchronized Pool createPool(String tag) throws FusionIOException {
		this.open();

		Pool pool = FusionIOAPI.fio_kv_create_pool(this, tag);
		if (pool == null) {
			throw new FusionIOException("Error while creating a new pool!",
				FusionIOAPI.fio_kv_get_last_error());
		}

		return pool;
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
