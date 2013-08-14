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


/**
 * This maps to the <tt>nvm_kv_store_info_t</tt> structure from the native
 * API.
 *
 * @author mpetazzoni
 */
public class StoreInfo {

	/** The store version, as set by the user with nvm_kv_open(). */
	private final int version;

	/** The number of pools on this FusionIO store. */
	private final int num_pools;

	/** The maximum number of pools supported in this store. */
	private final int max_pools;

	/** The requested expiry mode for this store. */
	private final int expiry_mode;

	/** An approximate number of key/value pairs across all pools in this store. */
	private final long num_keys;

	/** The free space, in bytes, on this device. */
	private final long free_space;

	public StoreInfo(int version, int num_pools, int max_pools,
			int expiry_mode, long num_keys, long free_space) {
		this.version = version;
		this.num_pools = num_pools;
		this.max_pools = max_pools;
		this.expiry_mode = expiry_mode;
		this.num_keys = num_keys;
		this.free_space = free_space;
	}

	/**
	 * Return the number of pools on the key/value store.
	 *
	 * <p>
	 * This returns the number of created, named pools and does <em>not</em>
	 * include the default pool (id: 0).
	 * </p>
	 */
	public int getNumPools() {
		return this.num_pools;
	}

	/**
	 * Return the maximum number of pools that can be created on the key/value
	 * store.
	 *
	 * <p>
	 * The maximum number of pools is always set to <tt>NVM_KV_MAX_POOLS
	 * (1048576)</tt> by the native API helper when opening a key/value store.
	 * </p>
	 */
	public int getMaxPools() {
		return this.max_pools;
	}

	public ExpiryMode getExpiryMode() {
		ExpiryMode[] modes = ExpiryMode.values();
		if (this.expiry_mode < 0 || this.expiry_mode >= modes.length) {
			return null;
		}

		return modes[this.expiry_mode];
	}

	/**
	 * Returns an approximation of the total number of key/value pairs on this
	 * FusionIO store.
	 */
	public long getNumKeys() {
		return this.num_keys;
	}

	/**
	 * Returns the available free space, in bytes, on the device.
	 *
	 * TODO(mpetazzoni): behavior with directFS?
	 */
	public long getFreeSpace() {
		return this.free_space;
	}
}
