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
#ifndef __FIO_KV_HELPER_H__
#define __FIO_KV_HELPER_H__

#include <nvm/nvm_kv.h>

#define FIO_SECTOR_ALIGNMENT        512
#define FIO_KV_MAX_POOLS						NVM_KV_MAX_POOLS // 1024
#define FIO_TAG_MAX_LENGTH					16

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
	const char *path;
	int fd;
	int64_t kv;
} fio_kv_store_t;

typedef struct {
	fio_kv_store_t *store;
	int id;
	char tag[FIO_TAG_MAX_LENGTH];
} fio_kv_pool_t;

typedef struct {
	uint32_t length;
	nvm_kv_key_t *bytes;
} fio_kv_key_t;

typedef struct {
	void *data;
	nvm_kv_key_info_t *info;
} fio_kv_value_t;


/**
 * Open a Fusion-IO device or directFS file for key/value store access.
 *
 * The user executing this program needs read-write access to the given device
 * or directFS file. If the given path starts with '/dev' it is considered to
 * point at a raw Fusion-IO device. Otherwise it is considered to be a directFS
 * file path, in which case the pool_id parameter is ignored.
 *
 * Args:
 *	 store (fio_kv_store_t *): An allocated fio_kv_store_t structure with the
 *		path member filled-in.
 *	version (uint32_t): A version number, controlled by the user, which is
 *		recorded in the device on the first time it is opened. Subsequent calls
 *		to fio_kv_open() validate this version number and return an error if the
 *		version mismatches.
 *	expiry_type (nvm_kv_expiry_t): The expiry type (disabled, arbitrary or
 *		global).
 *	expiry_time (uint32_t): The expiration time for global expiry mode, in
 *		seconds since the key/value pair insertion.
 * Returns:
 *	 Returns true if the operation was successful (and the fd and kv fields of
 *	 the fio_kv_store_t structure are filled in), false otherwise.
 */
bool fio_kv_open(fio_kv_store_t *store, const uint32_t version,
		const nvm_kv_expiry_t expiry_type, const uint32_t expiry_time);

/**
 * Close a key/value store.
 *
 * Closes and clears information in the fio_kv_store_t structure. It is the
 * responsibility of the caller to free to memory backing the structure.
 *
 * Args:
 *	 store (fio_kv_store_t *): The key/value store to close.
 */
void fio_kv_close(fio_kv_store_t *store);

/**
 * Returns information about the given key/value store.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 * Returns:
 *	A pointer to a populated nvm_kv_store_info_t structure containing metadata
 *	information about the key/value store.
 */
nvm_kv_store_info_t *fio_kv_get_store_info(fio_kv_store_t *store);

/**
 * Get or create a new pool in the given key/value store.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 *	tag (const char *): The pool name tag.
 * Returns:
 *	Returns a newly allocated fio_kv_pool_t structure describing the created
 *	pool.
 */
fio_kv_pool_t *fio_kv_get_or_create_pool(fio_kv_store_t *store,
		const char *tag);

/**
 * Get pool metadata information for up to the requested number of pools.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 *	pool_count (int *): A pointer to an integer that will contain the actual
 *		number of pools we got metadata about.
 * Returns:
 *	An array of initialized and filled-in fio_kv_pool_t structures, with the
 *	pool IDs and pool tags for each pool up to the number of requested pools.
 */
fio_kv_pool_t *fio_kv_get_all_pools(fio_kv_store_t *store,
		uint32_t *pool_count);

/**
 * Allocate sector-aligned memory to hold the given number of bytes.
 *
 * Args:
 *	 length (uint32_t): Size of the memory to allocate.
 * Returns:
 *	 A pointer to sector-aligned, allocated memory that can hold up to length
 *	 bytes.
 */
void *fio_kv_alloc(const uint32_t length);

/**
 * Free the memory allocated for the 'data' field of a fio_kv_value_t
 * structure.
 *
 * It is still the responsibility of the caller to manage the memory of the
 * other fields of that structure, in particular the 'info' field.
 *
 * Args:
 *	 value (fio_kv_value_t *): The value structure to free.
 */
void fio_kv_free_value(fio_kv_value_t *value);

/**
 * Retrieve a value's length, rounded up to the next sector, without any I/O.
 *
 * Args:
 *	pool (fio_kv_pool_t *): The key/value pool.
 *	key (fio_kv_key_t *): The key.
 * Returns:
 *	The value length, rounded up to the next sector.
 */
int fio_kv_get_value_len(const fio_kv_pool_t *pool, const fio_kv_key_t *key);

/**
 * Retrieve a key/value pair's exact metadata from the device.
 *
 * Args:
 *	pool (fio_kv_pool_t *): The key/value pool.
 *	key (fio_kv_key_t *): The key.
 * Returns:
 *	A pointer to an allocated and filled-in nvm_kv_key_info_t structure.
 */
nvm_kv_key_info_t *fio_kv_get_key_info(const fio_kv_pool_t *pool,
		const fio_kv_key_t *key);

/**
 * Retrieve the value associated with a given key in a key/value pool.
 *
 * Note that you must provide sector-aligned memory that will can contain the
 * requested number of bytes. Such memory can be obtained by calling
 * fio_kv_alloc().
 *
 * Args:
 *	pool (fio_kv_pool_t *): The key/value pool.
 *	key (fio_kv_key_t *): The key.
 *	value (fio_kv_value_t *): An allocated value structure to hold the
 *	  result.
 * Returns:
 *	 The number of bytes read, or -1 if the read failed.
 */
int fio_kv_get(const fio_kv_pool_t *pool, const fio_kv_key_t *key,
		const fio_kv_value_t *value);

/**
 * Insert (or replace) a key/value pair into a pool.
 *
 * Note that the value's data must be sector-aligned memory, which can be
 * obtained from the fio_kv_alloc() function.
 *
 * Args:
 *	 pool (fio_kv_pool_t *): The key/value pool.
 *	 key (fio_kv_key_t): The key.
 *	 value (fio_kv_value_t): The value.
 * Returns:
 *	 The number of bytes written, or -1 in case of an error.
 */
int fio_kv_put(const fio_kv_pool_t *pool, const fio_kv_key_t *key,
		const fio_kv_value_t *value);

/**
 * Tell whether a specific key exists in a key/value pool.
 *
 * Args:
 *	pool (fio_kv_pool_t *): The key/value pool.
 *	key (fio_kv_key_t *): The key of the mapping to check the existence of.
 *	info (nvm_kv_key_info_t *): An optional pointer to a nvm_kv_key_info_t
 *	  structure to fill with key/value pair information if it exists in the
 *	  key/value pool.
 * Returns:
 *	Returns true if a mapping for the given key exists in the key/value pool,
 *		false if it doesn't or if an error occured.
 */
bool fio_kv_exists(const fio_kv_pool_t *pool, const fio_kv_key_t *key,
		nvm_kv_key_info_t *info);

/**
 * Removes a key/value pair from a pool.
 *
 * Args:
 *	 pool (fio_kv_pool_t *): The key/value pool.
 *	 key (fio_kv_key_t *): The key to the mapping to remove.
 * Returns:
 *	 Returns true if the mapping was successfuly removed, false otherwise.
 */
bool fio_kv_delete(const fio_kv_pool_t *pool, const fio_kv_key_t *key);

/**
 * Removes all key/value pairs from all pools.
 *
 * All key/value pairs from all pools, including the default pools, are
 * removed. Pools are not deleted (use fio_kv_delete_pool() or
 * fio_kv_delete_all_pools() for that).
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 * Returns:
 *	Returns true if the operation was successful, false otherwise.
 */
bool fio_kv_delete_all(const fio_kv_store_t *store);

/**
 * Delete the given pool.
 *
 * Pool deletion is asynchronous. The pool will become unusable immediately but
 * the number of pools reported by fio_kv_get_store_info() will only be
 * decremented when the pool deletion is complete. Removing a pool removes all
 * the key/value pairs it contained.
 *
 * Args:
 *	pool (fio_kv_pool_t *): The pool to delete.
 * Returns:
 *	Returns true if the operation was successful, false otherwise.
 */
bool fio_kv_delete_pool(const fio_kv_pool_t *pool);

/**
 * Delete all user-created pools from the key/value store.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 * Returns:
 *	Returns true if the operation was successful, false otherwise.
 */
bool fio_kv_delete_all_pools(const fio_kv_store_t *store);

/**
 * Insert (or replace) a set of key/value pairs in one batch operation.
 *
 * Args:
 *	pool (fio_kv_pool_t *): The key/value pool.
 *	keys (fio_kv_key_t **): An array of pointers to the keys to insert.
 *	values (fio_kv_value_t **): An array of pointers to the corresponding
 *	  values.
 *	count (size_t): The number of key/value pairs.
 * Returns:
 *	Returns true if the batch insertion was successful, false otherwise.
 */
bool fio_kv_batch_put(const fio_kv_pool_t *pool, const fio_kv_key_t **keys,
		const fio_kv_value_t **values, const size_t count);

/**
 * Create an iterator on a key/value pool.
 *
 * Args:
 *	pool (fio_kv_pool_t *): The key/value pool.
 * Returns:
 *	Returns the new iterator's ID, or -1 if an iterator could not be
 *	  created.
 */
int fio_kv_iterator(const fio_kv_pool_t *pool);

/**
 * Move an iterator to the following element in a key/value pool.
 *
 * Args:
 *	pool (fio_kv_pool_t *): The key/value pool.
 *	iterator (int): The ID of the iterator to advance.
 * Returns:
 *	Returns true if the operation was successful, false otherwise.
 */
bool fio_kv_next(const fio_kv_pool_t *pool, const int iterator);

/**
 * Retrieve the key and value at the current iterator's position.
 *
 * Args:
 *	pool (fio_kv_pool_t *): The key/value pool.
 *	iterator (int): The ID of the iterator.
 *	key (fio_kv_key_t *): An allocated key structure to hold the current
 *	  key.
 *	value (fio_kv_value_t *): An allocated value structure to hold the
 * 	  current value.
 * Returns:
 *	Returns true if the operation was successful, false otherwise.
 */
bool fio_kv_get_current(const fio_kv_pool_t *pool, const int iterator,
		fio_kv_key_t *key, const fio_kv_value_t *value);

/**
 * End an iteration.
 *
 * Args:
 *	pool (fio_kv_pool_t *): The key/value pool.
 *	iterator (int): The ID of the iterator.
 * Returns:
 *	Returns true if the operation was successful, false otherwise.
 */
bool fio_kv_end_iteration(const fio_kv_pool_t *pool, const int iterator);

/**
 * Retrieve the last errno value.
 *
 * This method is obviously absolutely not thread-safe and cannot guarantee
 * that the returned value matches the last error.
 *
 * Returns:
 *	Returns the last errno value.
 */
int fio_kv_get_last_error(void);

#ifdef __cplusplus
}
#endif

#endif /* __FIO_KV_HELPER_H__ */
