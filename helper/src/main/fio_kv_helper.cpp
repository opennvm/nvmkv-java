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
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "fio_kv_helper.h"

/**
 * This is a glue library over FusionIO's key/value store API. It provides an
 * easier to use API that manages the details of FusionIO's API usage like
 * memory alignment, device management, etc into an all-encompassing and more
 * symetric API.
 *
 * It can be compiled into a shared library with the following command:
 *
 *   $ gcc -ansi -pedantic -pedantic-errors -fPIC -shared -Werror -lrt -ldl
 *       -lvsldpexp -L/usr/lib/fio -I/usr/lib/jvm/default-java/include/
 *       -o libfio_kv_helper.so fio_kv_helper.cpp
 *
 * Of course the FusionIO's development kit must be installed first.
 */


/**
 * Open a Fusion-IO device or directFS file for key/value store access.
 *
 * The user executing this program needs read-write access to the given device
 * or directFS file. If the given path starts with '/dev' it is considered to
 * point at a raw Fusion-IO device. Otherwise it is considered to be a directFS
 * file path, in which case the pool_id parameter is ignored.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store to open. The path structure
 *		member must be filled in.
 *	version (uint32_t): A version number, controlled by the user, which is
 *		recorded in the device on the first time it is opened. Subsequent calls
 *		to fio_kv_open() validate this version number and return an error if the
 *		version mismatches.
 *	expiry_type (nvm_kv_expiry_t): The expiry type (disabled, arbitrary or
 *		global).
 *	expiry_time (uint32_t): The expiration time for global expiry mode, in
 *		seconds since the key/value pair insertion.
 * Returns:
 *	Returns true if the operation succeeded, false otherwise.
 */
bool fio_kv_open(fio_kv_store_t *store, const uint32_t version,
		const nvm_kv_expiry_t expiry_type, const uint32_t expiry_time)
{
	int flags = O_RDWR | O_DIRECT;

	assert(store != NULL);
	assert(store->path != NULL);
	if (expiry_type == KV_GLOBAL_EXPIRY) {
		assert(expiry_time > 0);
	}

	if (strncmp(store->path, "/dev", 4) != 0) {
		flags |= O_LARGEFILE | O_CREAT;
	}

	store->fd = open(store->path, flags, S_IRUSR | S_IWUSR);
	if (store->fd < 0) {
		return 0;
	}

	store->kv = nvm_kv_open(store->fd, version, FIO_KV_MAX_POOLS,
			expiry_type);
	if (store->kv <= 0) {
		return 0;
	}

	if (expiry_type == KV_GLOBAL_EXPIRY &&
			nvm_kv_set_global_expiry(store->kv, expiry_time)) {
		fio_kv_close(store);
		return 0;
	}

	return 1;
}

/**
 * Close a key/value store.
 *
 * Closes and clears information in the fio_kv_store_t structure. It is the
 * responsibility of the caller to free to memory backing the structure.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store to close.
 */
void fio_kv_close(fio_kv_store_t *store)
{
	assert(store != NULL);
	assert(store->kv > 0);

	if (store->fd) {
		fsync(store->fd);
		close(store->fd);
	}

	store->fd = 0;
	store->kv = 0;
}

/**
 * Returns information about the given key/value store.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 * Returns:
 *	A pointer to a populated nvm_kv_store_info_t structure containing metadata
 *	information about the key/value store.
 */
nvm_kv_store_info_t *fio_kv_get_store_info(fio_kv_store_t *store)
{
	assert(store != NULL);
	assert(store->kv > 0);

	nvm_kv_store_info_t *info = (nvm_kv_store_info_t *)malloc(sizeof(nvm_kv_store_info_t));
	int ret = nvm_kv_get_store_info(store->kv, info);
	if (ret < 0) {
		free(info);
		return NULL;
	}

	return info;
}

/**
 * Get or create a new pool in the given key/value store.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 *	tag (const char *): The pool tag. Must be a properly NUL-terminated string
 *		no longer than FIO_TAG_MAX_LENGTH.
 * Returns:
 *	Returns a newly allocated fio_kv_pool_t structure describing the created
 *	pool.
 */
fio_kv_pool_t *fio_kv_get_or_create_pool(fio_kv_store_t *store,
		const char *tag)
{
	assert(store != NULL);
	assert(store->kv > 0);
	assert(strlen(tag) < FIO_TAG_MAX_LENGTH);

	int ret = nvm_kv_pool_create(store->kv, (nvm_kv_pool_tag_t *)tag);
	if (ret <= 0) {
		return NULL;
	}

	fio_kv_pool_t *pool = (fio_kv_pool_t *)malloc(sizeof(fio_kv_pool_t));
	pool->store = store;
	pool->id = ret;
	strcpy(pool->tag, tag);
	return pool;
}

/**
 * Get pool metadata information for up to the requested number of pools.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 *	pool_count (uint32_t *): A pointer to an integer that will contain the
 *		actual number of pools we got metadata about.
 * Returns:
 *	An array of initialized and filled-in fio_kv_pool_t structures, with the
 *	pool IDs and pool tags for each pool up to the number of requested pools.
 */
fio_kv_pool_t *fio_kv_get_all_pools(fio_kv_store_t *store, uint32_t *pool_count)
{
	fio_kv_pool_t *pools;
	nvm_kv_pool_metadata_t *metadata;
	int ret;

	assert(store != NULL);
	assert(store->kv > 0);
	assert(pool_count != NULL);

	metadata = (nvm_kv_pool_metadata_t *)calloc(FIO_KV_MAX_POOLS,
			sizeof(nvm_kv_pool_metadata_t));
	ret = nvm_kv_get_pool_metadata(store->kv, metadata, FIO_KV_MAX_POOLS, 1);
	if (ret < 0) {
		free(metadata);
		return NULL;
	}

	// TODO(mpetazzoni): remove when nvm_kv_get_pool_metadata() returns the
	// correct value.
	// *pool_count = ret;
	nvm_kv_store_info_t *info = fio_kv_get_store_info(store);
	*pool_count = info->num_pools;
	free(info);

	pools = (fio_kv_pool_t *)calloc(*pool_count, sizeof(fio_kv_pool_t));
	for (int i = 0; i < *pool_count ; i++) {
		pools[i].store = store;
		pools[i].id = metadata[i].pool_id;
		strcpy(pools[i].tag, (const char *)(metadata[i].pool_tag.pool_tag));
	}

	free(metadata);
	return pools;
}

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
bool fio_kv_delete_pool(const fio_kv_pool_t *pool)
{
	assert(pool != NULL);
	assert(pool->store != NULL);
	assert(pool->store->kv > 0);
	assert(pool->id > 1 && pool->id < NVM_KV_MAX_POOLS);

	return nvm_kv_pool_delete(pool->store->kv, pool->id) == 0;
}

/**
 * Delete all user-created pools from the key/value store.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 * Returns:
 *	Returns true if the operation was successful, false otherwise.
 */
bool fio_kv_delete_all_pools(const fio_kv_store_t *store)
{
	assert(store != NULL);
	assert(store->kv > 0);

	return nvm_kv_pool_delete(store->kv, -1) == 0;
}

/**
 * Allocate sector-aligned memory to hold the given number of bytes.
 *
 * Args:
 *	length (uint32_t): Size of the memory to allocate.
 * Returns:
 *	A pointer to sector-aligned, allocated memory that can hold up to length
 *	bytes.
 */
void *fio_kv_alloc(const uint32_t length)
{
	void *p;

	if (posix_memalign(&p, FIO_SECTOR_ALIGNMENT,
			length + FIO_SECTOR_ALIGNMENT -
				(length % FIO_SECTOR_ALIGNMENT))) {
        return NULL;
    }

	return p;
}

/**
 * Free the memory allocated for the 'data' field of a fio_kv_value_t
 * structure.
 *
 * It is still the responsibility of the caller to manage the memory of the
 * other fields of that structure, in particular the 'info' field.
 *
 * Args:
 *	value (fio_kv_value_t *): The value structure to free.
 */
void fio_kv_free_value(fio_kv_value_t *value)
{
	assert(value != NULL);

	free(value->data);
	value->data = NULL;
}

/**
 * Retrieve a value's length, rounded up to the next sector, without any I/O.
 *
 * Args:
 *	pool (fio_kv_pool_t *): The key/value pool.
 *	key (fio_kv_key_t *): The key.
 * Returns:
 *	The value length, rounded up to the next sector.
 */
int fio_kv_get_value_len(const fio_kv_pool_t *pool, const fio_kv_key_t *key)
{
	assert(pool != NULL);
	assert(pool->store != NULL);
	assert(pool->store->kv > 0);
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);
	assert(key != null);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);

	int len = nvm_kv_get_val_len(pool->store->kv, pool->id,
			key->bytes, key->length);
	return len < NVM_KV_MAX_VALUE_SIZE ? len : NVM_KV_MAX_VALUE_SIZE;
}

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
		const fio_kv_key_t *key)
{
	assert(pool != NULL);
	assert(pool->store != NULL);
	assert(pool->store->kv > 0);
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);
	assert(key != null);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);

	nvm_kv_key_info_t *info = (nvm_kv_key_info_t *)malloc(sizeof(nvm_kv_key_info_t));
	if (nvm_kv_get_key_info(pool->store->kv, pool->id,
			key->bytes, key->length, info)) {
		free(info);
		info = NULL;
	}

	return info;
}

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
		const fio_kv_value_t *value)
{
	assert(pool != NULL);
	assert(pool->store != NULL);
	assert(pool->store->kv > 0);
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);
	assert(value != NULL);
	assert(value->data != NULL);
	assert(value->info != NULL);
	assert(value->info->value_len <= NVM_KV_MAX_VALUE_SIZE);

	return nvm_kv_get(pool->store->kv, pool->id,
			key->bytes, key->length,
			value->data, value->info->value_len, false,
			value->info);
}

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
		const fio_kv_value_t *value)
{
	assert(pool != NULL);
	assert(pool->store != NULL);
	assert(pool->store->kv > 0);
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);
	assert(value != NULL);
	assert(value->data != NULL);
	assert(value->info != NULL);
	assert(value->info->value_len <= NVM_KV_MAX_VALUE_SIZE);

	return nvm_kv_put(pool->store->kv, pool->id,
			key->bytes, key->length,
			value->data, value->info->value_len,
			value->info->expiry, true, 0);
}

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
		nvm_kv_key_info_t *info)
{
	assert(pool != NULL);
	assert(pool->store != NULL);
	assert(pool->store->kv > 0);
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);

	// TODO(mpetazzoni): remove once nvm_kv_exists() no longer segfaults when
	// passed a NULL nvm_kv_key_info_t parameter.
	nvm_kv_key_info_t _info;
	if (info == NULL) {
		info = &_info;
	}

	return nvm_kv_exists(pool->store->kv, pool->id,
			key->bytes, key->length, info) == 1;
}

/**
 * Removes a key/value pair from a pool.
 *
 * Args:
 *	 pool (fio_kv_pool_t *): The key/value pool.
 *	 key (fio_kv_key_t *): The key to the mapping to remove.
 * Returns:
 *	 Returns true if the mapping was successfuly removed, false otherwise.
 */
bool fio_kv_delete(const fio_kv_pool_t *pool, const fio_kv_key_t *key)
{
	assert(pool != NULL);
	assert(pool->store != NULL);
	assert(pool->store->kv > 0);
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);

	return nvm_kv_delete(pool->store->kv, pool->id,
			key->bytes, key->length) == 0;
}

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
bool fio_kv_delete_all(const fio_kv_store_t *store)
{
	assert(store != NULL);
	assert(store->kv > 0);

	return nvm_kv_delete_all(store->kv) == 0;
}

/**
 * Prepare a nvm_kv_iovec_t structure array for batch operations.
 *
 * Args:
 *	keys (fio_kv_key_t **): An array of pointer to keys.
 *	values (fio_kv_value_t **): An array of pointer to the corresponding
 *	  values (optional).
 *	count (size_t): The number of elements.
 * Returns:
 *	Returns an allocated, populated array of count nvm_kv_iovec_t structures
 *	to be used for batch operations.
 */
nvm_kv_iovec_t *_fio_kv_prepare_batch(const fio_kv_key_t **keys,
		const fio_kv_value_t **values, const size_t count)
{
	nvm_kv_iovec_t *v;

	assert(keys != NULL);
	assert(count > 0);

	v = (nvm_kv_iovec_t *)calloc(count, sizeof(nvm_kv_iovec_t));
	for (int i=0; i<count; i++) {
		assert(keys[i] != NULL);
		assert(keys[i]->length >= 1 && keys[i]->length <= NVM_KV_MAX_KEY_SIZE);
		assert(keys[i]->bytes != NULL);

		v[i].key = keys[i]->bytes;
		v[i].key_len = keys[i]->length;

		if (values != NULL) {
			assert(values[i] != NULL);
			assert(values[i]->data != NULL);
			assert(values[i]->info != NULL);

			v[i].value = values[i]->data;
			v[i].value_len = values[i]->info->value_len;
			v[i].expiry = values[i]->info->expiry;
			v[i].gen_count = values[i]->info->gen_count;
			v[i].replace = 1;
		}
	}

	return v;
}

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
		const fio_kv_value_t **values, const size_t count)
{
	nvm_kv_iovec_t *v;
	int ret;

	assert(pool != NULL);
	assert(pool->store != NULL);
	assert(pool->store->kv > 0);
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);
	assert(keys != NULL);
	assert(values != NULL);

	v = _fio_kv_prepare_batch(keys, values, count);
	ret = nvm_kv_batch_put(pool->store->kv, pool->id, v, count);
	free(v);

	return ret == 0;
}

/**
 * Create an iterator on a key/value pool.
 *
 * Args:
 *	pool (fio_kv_pool_t *): The key/value pool.
 * Returns:
 *	Returns the new iterator's ID, or -1 if an iterator could not be
 *	  created.
 */
int fio_kv_iterator(const fio_kv_pool_t *pool)
{
	assert(pool != NULL);
	assert(pool->store != NULL);
	assert(pool->store->kv > 0);
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);

	return nvm_kv_begin(pool->store->kv, pool->id);
}

/**
 * Move an iterator to the following element in a key/value pool.
 *
 * Args:
 *	pool (fio_kv_pool_t *): The key/value pool.
 *	iterator (int): The ID of the iterator to advance.
 * Returns:
 *	Returns true if the operation was successful, false otherwise.
 */
bool fio_kv_next(const fio_kv_pool_t *pool, const int iterator)
{
	assert(pool != NULL);
	assert(pool->store != NULL);
	assert(pool->store->kv > 0);
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);
	assert(iterator >= 0);

	return nvm_kv_next(pool->store->kv, iterator) == 0;
}

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
		fio_kv_key_t *key, const fio_kv_value_t *value)
{
	assert(pool != NULL);
	assert(pool->store != NULL);
	assert(pool->store->kv > 0);
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);
	assert(iterator >= 0);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);
	assert(value != NULL);
	assert(value->data != NULL);
	assert(value->info != NULL);

	return nvm_kv_get_current(pool->store->kv, iterator,
			key->bytes, &(key->length),
			value->data, value->info->value_len,
			value->info) >= 0;
}

/**
 * End an iteration.
 *
 * Args:
 *	pool (fio_kv_pool_t *): The key/value pool.
 *	iterator (int): The ID of the iterator.
 * Returns:
 *	Returns true if the operation was successful, false otherwise.
 */
bool fio_kv_end_iteration(const fio_kv_pool_t *pool, const int iterator)
{
	assert(pool != NULL);
	assert(pool->store != NULL);
	assert(pool->store->kv > 0);
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);
	assert(iterator >= 0);

	return nvm_kv_iteration_end(pool->store->kv, iterator) == 0;
}

/**
 * Retrieve the last errno value.
 *
 * This method is obviously absolutely not thread-safe and cannot guarantee
 * that the returned value matches the last error.
 *
 * Returns:
 *	Returns the last errno value.
 */
int fio_kv_get_last_error(void)
{
	return errno;
}
