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
public class KeyValueInfo {

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
	}

	public KeyValueInfo(int pool_id, int key_len, int value_len, int expiry,
		int gen_count) {
		this.pool_id = pool_id;
		this.key_len = key_len;
		this.value_len = value_len;
		this.expiry = expiry;
		this.gen_count = gen_count;
	}
};
