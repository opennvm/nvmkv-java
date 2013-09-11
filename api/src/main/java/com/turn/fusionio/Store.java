/**
 * Copyright (C) 2012-2013 Turn Inc.  All Rights Reserved.
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
 * - Neither the name Turn Inc. nor the names of its contributors may be used
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

/**
 * Represents a FusionIO-based key/value store.
 *
 * <p>
 * The public fields in this class map to the <tt>fio_kv_store_t</tt>
 * structure in the helper library.
 * </p>
 *
 * @author mpetazzoni
 */
public class Store implements Closeable {

	public static final int FUSION_IO_MAX_POOLS = 1048576;

	/** The path to the FusionIO device or directFS file. */
	public final String path;

	/** The file descriptor bound to the FusionIO device. */
	public final int fd;

	/** The KV store ID as understood by FusionIO's API. */
	public final long kv;

	private final int version;
	private final ExpiryMode expiryMode;
	private final int expiryTime;

	private volatile boolean opened;

	/**
	 * Instantiate a new FusionIO Store object.
	 *
	 * @param path The FusionIO device file or directFS file path.
	 * @param version The user-defined FusionIO store version. Opening a
	 *	previously created store with a different version number will fail.
	 * @param expiryMode The key/value pair expiration policy.
	 * @param expiryTime Only used for {@link ExpiryMode.GLOBAL_EXPIRY},
	 *	defines the key/value pair TTL in seconds after insertion.
	 */
	Store(String path, int version, ExpiryMode expiryMode, int expiryTime) {
		if (path == null ||
			version < 0 ||
			expiryMode == null ||
			(expiryMode == ExpiryMode.GLOBAL_EXPIRY && expiryTime <= 0)) {
			throw new IllegalArgumentException();
		}

		this.path = path;
		this.version = version;
		this.expiryMode = expiryMode;
		this.expiryTime = expiryTime;

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
			if (!FusionIOAPI.fio_kv_open(this, this.version, this.expiryMode,
					this.expiryTime)) {
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
	 * Removes all key/value pairs from all pools, including the default pool.
	 *
	 * @throws FusionIOException In an error occured during removal.
	 */
	public void clear() throws FusionIOException {
		if (!this.isOpened()) {
			throw new IllegalStateException("Key/value store is not opened!");
		}

		if (!FusionIOAPI.fio_kv_delete_all(this)) {
			throw new FusionIOException("Error removing all key/value pairs!",
				FusionIOAPI.fio_kv_get_last_error());
		}
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
		if (!this.isOpened()) {
			throw new IllegalStateException("Key/value store is not opened!");
		}

		return FusionIOAPI.fio_kv_get_store_info(this);
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
	public Pool getOrCreatePool(String tag)
			throws FusionIOException {
		if (!this.isOpened()) {
			throw new IllegalStateException("Key/value store is not opened!");
		}

		if (tag != null && tag.length() > Pool.FUSION_IO_MAX_POOL_TAG_LENGTH) {
			throw new FusionIOException("Invalid pool tag '" + tag + "'!");
		}

		Pool pool = FusionIOAPI.fio_kv_get_or_create_pool(this, tag);
		if (pool == null) {
			throw new FusionIOException("Error while creating a new pool!",
				FusionIOAPI.fio_kv_get_last_error());
		}

		return pool;
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
		if (!this.isOpened()) {
			throw new IllegalStateException("Key/value store is not opened!");
		}

		return new Pool(this, id, null);
	}

	/**
	 * Return an array of all pools in this key/value store.
	 *
	 * @throws FusionIOException
	 */
	public Pool[] getAllPools() throws FusionIOException {
		if (!this.isOpened()) {
			throw new IllegalStateException("Key/value store is not opened!");
		}

		Pool[] pools = FusionIOAPI.fio_kv_get_all_pools(this);
		if (pools == null) {
			throw new FusionIOException("Error while gathering pool information!",
				FusionIOAPI.fio_kv_get_last_error());
		}

		return pools;
	}

	/**
	 * Delete a pool from the key/value store.
	 *
	 * <p>
	 * This is an asynchronous operation. All key/value pairs in the pool will
	 * be removed. The default pool cannot be destroyed.
	 * </p>
	 *
	 * @throws FusionIOException If the operation failed.
	 * @throws IllegalArgumentException If you try to remove the default pool.
	 */
	public void deletePool(Pool pool) throws FusionIOException {
		if (!this.isOpened()) {
			throw new IllegalStateException("Key/value store is not opened!");
		}

		if (pool.id == Pool.FUSION_IO_DEFAULT_POOL_ID) {
			throw new IllegalArgumentException("You cannot remove the default pool!");
		}

		if (!FusionIOAPI.fio_kv_delete_pool(pool)) {
			throw new FusionIOException("Error while deleting pool " + this,
				FusionIOAPI.fio_kv_get_last_error());
		}
	}

	/**
	 * Delete all user-created pools in this store.
	 *
	 * <p>
	 * This is an asynchronous operation. Pools will no longer be accessible
	 * after this method is called, but the number of pools reported in the
	 * store information will only be decremented once pool deletion is
	 * complete. The default pool remains untouched.
	 * </p>
	 *
	 * @throws FusionIOException If the operation failed.
	 */
	public void deleteAllPools() throws FusionIOException {
		if (!this.isOpened()) {
			throw new IllegalStateException("Key/value store is not opened!");
		}

		if (!FusionIOAPI.fio_kv_delete_all_pools(this)) {
			throw new FusionIOException("Error while deleting pools!",
				FusionIOAPI.fio_kv_get_last_error());
		}
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
