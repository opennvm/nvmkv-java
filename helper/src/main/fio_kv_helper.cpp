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
 *       -lvsldpexp -L/usr/lib/fio -o libfio_kv_helper.so fio_kv_helper.cpp
 *
 * Of course the FusionIO's development kit must be installed first.
 */

/**
 * Open a Fusion-IO device for key/value store access.
 *
 * The user executing this program needs read-write access to the given device.
 *
 * Args:
 *	 device (const char *): Path to the Fusion-IO device file.
 * Returns:
 *	 Returns a fio_kv_store_t structure containing the file descriptor
 *	 associated with the open device and the KV store ID used by the KV SDK
 *	 API.
 */
fio_kv_store_t *fio_kv_open(const char *device, int pool_id)
{
	fio_kv_store_t *store;

	assert(device != NULL);
	assert(pool_id >= 0);

	store	= (fio_kv_store_t *)calloc(1, sizeof(fio_kv_store_t));

	store->fd = open(device, O_RDWR);
	if (store->fd < 0) {
		free(store);
		return NULL;
	}

	store->kv = kv_create(store->fd, FIO_KV_API_VERSION, FIO_KV_MAX_POOLS, false);
	if (store->kv <= 0) {
		free(store);
		return NULL;
	}

	store->pool = pool_id;
	return store;
}

/**
 * Close a key/value store.
 *
 * Closes and clears information in the fio_kv_store_t structure. It is the
 * responsibility of the caller to free to memory backing the structure.
 *
 * Args:
 *	 store (fio_kv_store_t *): The key/value store to close.
 */
void fio_kv_close(fio_kv_store_t *store)
{
	assert(store != NULL);

	// TODO: check return value of close(). See close(2).
	close(store->fd);
	store->fd = 0;
	store->kv = 0;
}

/**
 * Allocate sector-aligned memory to hold the given number of bytes.
 *
 * Args:
 *	 length (uint32_t): Size of the memory to allocate.
 * Returns:
 *	 A pointer to sector-aligned, allocated memory that can hold up to length
 *	 bytes.
 */
void *fio_kv_alloc(uint32_t length)
{
	void *p;

	posix_memalign(&p, FIO_SECTOR_ALIGNMENT,
			length + FIO_SECTOR_ALIGNMENT -
				(length % FIO_SECTOR_ALIGNMENT));

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
 *	 value (fio_kv_value_t *): The value structure to free.
 */
void fio_kv_free_value(fio_kv_value_t *value)
{
	assert(value != NULL);

	free(value->data);
	value->data = NULL;
}

/**
 * Retrieve the value associated with a given key in a key/value store.
 *
 * Note that you must provide sector-aligned memory that will can contain the
 * requested number of bytes. Such memory can be obtained by calling
 * fio_kv_alloc().
 *
 * Args:
 *	 store (fio_kv_store_t *): The key/value store.
 *	 key (fio_kv_key_t *): The key.
 *	 value (fio_kv_value_t *): An allocated value structure to hold the
 *		 result.
 * Returns:
 *	 The number of bytes read, or -1 if the read failed.
 */
int fio_kv_get(fio_kv_store_t *store, fio_kv_key_t *key,
		fio_kv_value_t *value)
{
	int ret;

	assert(store != NULL);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);
	assert(value != NULL);
	assert(value->data != NULL);
	assert(value->info != NULL);

	ret = kv_get(store->kv, store->pool,
			key->bytes, key->length,
			value->data, value->info->value_len,
			value->info);
	if (ret < 0) {
		return ret;
	}

	return ret;
}

/**
 * Insert (or replace) a key/value pair into the store.
 *
 * Note that the value's data must be sector-aligned memory, which can be
 * obtained from the fio_kv_alloc() function.
 *
 * Args:
 *	 store (fio_kv_store_t *): A pointer to a opened KV store.
 *	 key (fio_kv_key_t): The key.
 *	 value (fio_kv_value_t): The value.
 * Returns:
 *	 The number of bytes written, or -1 in case of an error.
 */
int fio_kv_put(fio_kv_store_t *store, fio_kv_key_t *key,
		fio_kv_value_t *value)
{
	assert(store != NULL);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);
	assert(value != NULL);
	assert(value->data != NULL);
	assert(value->info != NULL);

	return kv_put(store->kv, store->pool,
			key->bytes, key->length,
			value->data, value->info->value_len,
			value->info->expiry, true, 0);
}

/**
 * Tell whether a specific key exists in a key/value store.
 *
 * Args:
 *	 store (fio_kv_store_t *): The key/value store.
 *	 key (fio_kv_key_t *): The key of the mapping to check the existence of.
 * Returns:
 *	 Returns true if a mapping for the given key exists in the key/value store.
 */
bool fio_kv_exists(fio_kv_store_t *store, fio_kv_key_t *key)
{
	assert(store != NULL);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);

	return kv_exists(store->kv, store->pool, key->bytes, key->length);
}

/**
 * Remove a key/value pair from the key/value store.
 *
 * Args:
 *	 store (fio_kv_store_t *): The key/value store.
 *	 key (fio_kv_key_t *): The key to the mapping to remove.
 * Returns:
 *	 Returns true if the mapping was successfuly removed, false otherwise.
 */
bool fio_kv_delete(fio_kv_store_t *store, fio_kv_key_t *key)
{
	assert(store != NULL);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);

	return kv_delete(store->kv, store->pool, key->bytes, key->length) == 0;
}


/**
 * Prepare a kv_iovec_t structure array for batch operations.
 *
 * Args:
 *   keys (fio_kv_key_t *): An array of keys.
 *   values (fio_kv_value_t *): An array of corresponding values (optional).
 *   count (size_t): The number of elements.
 * Returns:
 *   Returns an allocated, populated array of count kv_iovec_t structures to be
 *   used for batch operations.
 */
kv_iovec_t *_fio_kv_prepare_batch(fio_kv_key_t *keys, fio_kv_value_t *values,
		size_t count)
{
	kv_iovec_t *kv_iov;

	assert(keys != NULL);
	assert(count > 0);

	kv_iov = (kv_iovec_t *)calloc(count, sizeof(kv_iovec_t));
	for (int i=0; i<count; i++) {
		assert(keys[i].length >= 1 && keys[i].length <= KV_MAX_KEY_SIZE);
		assert(keys[i].bytes != NULL);

		kv_iov[i].key = keys[i].bytes;
		kv_iov[i].key_len = keys[i].length;

		if (values != NULL) {
			assert(values[i].data != NULL);
			assert(values[i].info != NULL);

			kv_iov[i].value = values[i].data;
			kv_iov[i].value_len = values[i].info->value_len;
			kv_iov[i].expiry = values[i].info->expiry;
			kv_iov[i].gen_count = values[i].info->gen_count;
			kv_iov[i].replace = 1;
		}
	}

	return kv_iov;
}

/**
 * Retrieve a set of values corresponding to the given set of keys in one
 * batch operation.
 *
 * Note that you must provide sector-aligned memory of the appropriate size for
 * each value.
 *
 * Args:
 *   store (fio_kv_store_t *): The key/value store.
 *   keys (fio_kv_key_t *): An array of keys to retrieve.
 *   values (fio_kv_value_t *): An array of allocated value structures to hold
 *     the results.
 *   count (size_t): The number of key/value pairs.
 * Returns:
 *   Returns true if the batch retrieval was successful, false otherwise.
 */
bool fio_kv_batch_get(fio_kv_store_t *store, fio_kv_key_t *keys,
		fio_kv_value_t *values, size_t count)
{
	kv_iovec_t *kv_iov;
	int ret;

	assert(store != NULL);
	assert(values != NULL);

	kv_iov = _fio_kv_prepare_batch(keys, values, count);
	ret = kv_batch_get(store->kv, store->pool, kv_iov, count);
	free(kv_iov);

	return ret == 0;
}

/**
 * Insert (or replace) a set of key/value pairs in one batch operation.
 *
 * Args:
 *   store (fio_kv_store_t *): The key/value store.
 *   keys (fio_kv_key_t *): An array of keys to insert.
 *   values (fio_kv_value_t *): An array of corresponding values.
 *   count (size_t): The number of key/value pairs.
 * Returns:
 *   Returns true if the batch insertion was successful, false otherwise.
 */
bool fio_kv_batch_put(fio_kv_store_t *store, fio_kv_key_t *keys,
		fio_kv_value_t *values, size_t count)
{
	kv_iovec_t *kv_iov;
	int ret;

	assert(store != NULL);
	assert(values != NULL);

	kv_iov = _fio_kv_prepare_batch(keys, values, count);
	ret = kv_batch_put(store->kv, store->pool, kv_iov, count);
	free(kv_iov);

	return ret == 0;
}

/**
 * Remove a set of key/value pairs in one batch operation.
 *
 * Args:
 *   store (fio_kv_store_t *): The key/value store.
 *   keys (fio_kv_key_t *): An array of keys to remove.
 *   count (size_t): The number of key/value pairs.
 * Returns:
 *   Returns true if the batch insertion was successful, false otherwise.
 */
bool fio_kv_batch_delete(fio_kv_store_t *store, fio_kv_key_t *keys,
		size_t count)
{
	kv_iovec_t *kv_iov;
	int ret;

	assert(store != NULL);

	kv_iov = _fio_kv_prepare_batch(keys, NULL, count);
	ret = kv_batch_delete(store->kv, store->pool, kv_iov, count);
	free(kv_iov);

	return ret == 0;
}

/**
 * Create an iterator on a key/value store.
 *
 * Args:
 *   store (fio_kv_store_t *): The key/value store.
 * Returns:
 *   Returns the new iterator's ID, or -1 if an iterator could not be created.
 */
int fio_kv_iterator(fio_kv_store_t *store)
{
	assert(store != NULL);

	return kv_begin(store->kv, store->pool);
}

/**
 * Move an iterator to the following element in a key/value store.
 *
 * Args:
 *   store (fio_kv_store_t *): The key/value store.
 *   iterator (int): The ID of the iterator to advance.
 * Returns:
 *   Returns true if the operation was successful, false otherwise.
 */
bool fio_kv_next(fio_kv_store_t *store, int iterator)
{
	assert(store != NULL);
	assert(iterator >= 0);

	return kv_next(store->kv, store->pool, iterator) == 0;
}

/**
 * Retrieve the key and value at the current iterator's position.
 *
 * Args:
 *   store (fio_kv_store_t *): The key/value store.
 *   iterator (int): The ID of the iterator.
 *   key (fio_kv_key_t *): An allocated key structure to hold the current key.
 *   value (fio_kv_value_t *): An allocated value structure to hold the current
 *		value.
 * Returns:
 *   Returns true if the operation was successful, false otherwise.
 */
bool fio_kv_get_current(fio_kv_store_t *store, int iterator, fio_kv_key_t *key,
		fio_kv_value_t *value)
{
	assert(store != NULL);
	assert(iterator >= 0);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);
	assert(value != NULL);
	assert(value->data != NULL);
	assert(value->info != NULL);

	return kv_get_current(store->kv, iterator,
			key->bytes, &(key->length),
			value->data, value->info->value_len,
			value->info) >= 0;
}

/**
 * Retrieve the last errno value.
 *
 * This method is obviously absolutely not thread-safe and cannot guarantee
 * that the returned value matches the last error.
 *
 * Returns:
 *	 Returns the last errno value.
 */
int fio_kv_get_last_error(void)
{
	return errno;
}
