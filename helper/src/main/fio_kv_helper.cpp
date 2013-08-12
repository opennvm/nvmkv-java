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
 * Returns:
 *	Returns true if the operation succeeded, false otherwise.
 */
bool fio_kv_open(fio_kv_store_t *store)
{
	int flags = O_RDWR | O_DIRECT;

	assert(store != NULL);
	assert(store->path != NULL);

	if (strncmp(store->path, "/dev", 4) != 0) {
		flags |= O_LARGEFILE | O_CREAT;
	}

	store->fd = open(store->path, flags, S_IRUSR | S_IWUSR);
	if (store->fd < 0) {
		return 0;
	}

	store->kv = nvm_kv_open(store->fd, FIO_KV_API_VERSION,
			NVM_KV_MAX_POOLS, KV_ARBITRARY_EXPIRY);
	if (store->kv <= 0) {
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

	if (store->fd) {
		// TODO: check return value of close(). See close(2).
		close(store->fd);
	}

	store->fd = 0;
	store->kv = 0;
}

/**
 * Destroy a key/value store.
 *
 * WARNING: this method is data destructive. All pools on the given store, as
 * well as all the keys and values they contain will be destroyed and not
 * recoverable. Use with care!
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store to destroy.
 */
bool fio_kv_destroy(fio_kv_store_t *store)
{
	assert(store != NULL);
	int ret = nvm_kv_destroy(store->kv);
	fio_kv_close(store);
	return ret == 0;
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

	nvm_kv_store_info_t *info = (nvm_kv_store_info_t *)malloc(sizeof(nvm_kv_store_info_t));
	int ret = nvm_kv_get_store_info(store->kv, info);
	if (ret < 0) {
		free(info);
		return NULL;
	}

	return info;
}

/**
 * Create a new pool in the given key/value store.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 *	tag (nvm_kv_pool_tag_t *): The pool name tag.
 * Returns:
 *	Returns a newly allocated fio_kv_pool_t structure describing the created
 *	pool.
 */
fio_kv_pool_t *fio_kv_create_pool(fio_kv_store_t *store,
		nvm_kv_pool_tag_t *tag)
{
	assert(store != NULL);
	int ret = nvm_kv_pool_create(store->kv, tag);
	if (ret <= 0) {
		return NULL;
	}

	fio_kv_pool_t *pool = (fio_kv_pool_t *)malloc(sizeof(fio_kv_pool_t));
	pool->store = store;
	pool->id = ret;
	pool->tag = tag;
	return pool;
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
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);
	assert(key != null);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);

	return nvm_kv_get_val_len(pool->store->kv, pool->id,
			key->bytes, key->length);
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
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);
	assert(value != NULL);
	assert(value->data != NULL);
	assert(value->info != NULL);

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
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);
	assert(value != NULL);
	assert(value->data != NULL);
	assert(value->info != NULL);

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
 *	Returns 1 if a mapping for the given key exists in the key/value pool, 0
 *	  if it doesn't or -1 if an error occured.
 */
int fio_kv_exists(const fio_kv_pool_t *pool, const fio_kv_key_t *key,
		nvm_kv_key_info_t *info)
{
	assert(pool != NULL);
	assert(pool->store != NULL);
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
			key->bytes, key->length, info);
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
	assert(pool->id >= 0 && pool->id < NVM_KV_MAX_POOLS);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);

	return nvm_kv_delete(pool->store->kv, pool->id,
			key->bytes, key->length) == 0;
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


/* ----------------------------------------------------------------------------
 *
 * JNI binding methods, wrappers around the fio_kv_* methods exposed to
 * the Java world.
 *
 * ----------------------------------------------------------------------------
 */

#ifdef __FIO_KV_HELPER_USE_JNI__
#include "com_turn_fusionio_FusionIOAPI.h"

static jclass _store_cls;
static jfieldID _store_field_path,
                _store_field_fd,
                _store_field_kv;

static jclass _storeinfo_cls;
static jmethodID _storeinfo_cstr;
static jfieldID _storeinfo_field_version,
                _storeinfo_field_num_pools,
                _storeinfo_field_max_pools,
                _storeinfo_field_expiry_mode,
                _storeinfo_field_num_keys,
                _storeinfo_field_free_space;

static jclass _pool_cls;
static jmethodID _pool_cstr;
static jfieldID _pool_field_store,
                _pool_field_id,
                _pool_field_tag;

static jclass _key_cls;
static jmethodID _key_cstr;
static jfieldID _key_field_length,
                _key_field_bytes;

static jclass _value_cls;
static jmethodID _value_cstr;
static jfieldID _value_field_data,
                _value_field_info;

static jclass _kvinfo_cls;
static jmethodID _kvinfo_cstr;
static jfieldID _kvinfo_field_pool_id,
                _kvinfo_field_key_len,
                _kvinfo_field_value_len,
                _kvinfo_field_expiry,
                _kvinfo_field_gen_count;

/**
 * Initializes the JNI class, method and field IDs.
 */
JNIEXPORT void JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1init_1jni_1cache
  (JNIEnv *env, jclass cls)
{
	jclass _cls;

	_cls = env->FindClass("com/turn/fusionio/Store");
	assert(_cls != NULL);
	_store_cls = (jclass) env->NewGlobalRef(_cls);
	env->DeleteLocalRef(_cls);
	_store_field_path = env->GetFieldID(_store_cls, "path", "Ljava/lang/String;");
	_store_field_fd   = env->GetFieldID(_store_cls, "fd", "I");
	_store_field_kv   = env->GetFieldID(_store_cls, "kv", "J");

	_cls = env->FindClass("com/turn/fusionio/StoreInfo");
	assert(_cls != NULL);
	_storeinfo_cls = (jclass) env->NewGlobalRef(_cls);
	env->DeleteLocalRef(_cls);
	_storeinfo_cstr = env->GetMethodID(_storeinfo_cls, "<init>", "(IIIIJJ)V");
	_storeinfo_field_version     = env->GetFieldID(_storeinfo_cls, "version", "I");
	_storeinfo_field_num_pools   = env->GetFieldID(_storeinfo_cls, "num_pools", "I");
	_storeinfo_field_max_pools   = env->GetFieldID(_storeinfo_cls, "max_pools", "I");
	_storeinfo_field_expiry_mode = env->GetFieldID(_storeinfo_cls, "expiry_mode", "I");
	_storeinfo_field_num_keys    = env->GetFieldID(_storeinfo_cls, "num_keys", "J");
	_storeinfo_field_free_space  = env->GetFieldID(_storeinfo_cls, "free_space", "J");

	_cls = env->FindClass("com/turn/fusionio/Pool");
	assert(_cls != NULL);
	_pool_cls = (jclass) env->NewGlobalRef(_cls);
	env->DeleteLocalRef(_cls);
	_pool_cstr = env->GetMethodID(_pool_cls, "<init>", "(Lcom/turn/fusionio/Store;ILjava/lang/String;)V");
	_pool_field_store = env->GetFieldID(_pool_cls, "store", "Lcom/turn/fusionio/Store;");
	_pool_field_id    = env->GetFieldID(_pool_cls, "id",    "I");
	_pool_field_tag   = env->GetFieldID(_pool_cls, "tag",   "Ljava/lang/String;");

	_cls = env->FindClass("com/turn/fusionio/Key");
	assert(_cls != NULL);
	_key_cls = (jclass) env->NewGlobalRef(_cls);
	env->DeleteLocalRef(_cls);
	_key_cstr = env->GetMethodID(_key_cls, "<init>", "()V");
	_key_field_length = env->GetFieldID(_key_cls, "length", "I");
	_key_field_bytes  = env->GetFieldID(_key_cls, "bytes",
			"Ljava/nio/ByteBuffer;");

	_cls = env->FindClass("com/turn/fusionio/Value");
	assert(_cls != NULL);
	_value_cls = (jclass) env->NewGlobalRef(_cls);
	env->DeleteLocalRef(_cls);
	_value_field_data = env->GetFieldID(_value_cls, "data",
			"Ljava/nio/ByteBuffer;");
	_value_field_info = env->GetFieldID(_value_cls, "info",
			"Lcom/turn/fusionio/KeyValueInfo;");

	_cls = env->FindClass("com/turn/fusionio/KeyValueInfo");
	assert(_cls != NULL);
	_kvinfo_cls = (jclass) env->NewGlobalRef(_cls);
	env->DeleteLocalRef(_cls);
	_kvinfo_cstr = env->GetMethodID(_kvinfo_cls, "<init>", "(IIIII)V");
	_kvinfo_field_pool_id   = env->GetFieldID(_kvinfo_cls, "pool_id",   "I");
	_kvinfo_field_key_len   = env->GetFieldID(_kvinfo_cls, "key_len",   "I");
	_kvinfo_field_value_len = env->GetFieldID(_kvinfo_cls, "value_len", "I");
	_kvinfo_field_expiry    = env->GetFieldID(_kvinfo_cls, "expiry",    "I");
	_kvinfo_field_gen_count = env->GetFieldID(_kvinfo_cls, "gen_count", "I");
}

/**
 * Convert a Store object into a fio_kv_store_t structure.
 *
 * A new fio_kv_store_t structure is allocated and populated. It will need to
 * be freed by the caller when it is no longer needed.
 */
fio_kv_store_t *__jobject_to_fio_kv_store(JNIEnv *env, jobject _store)
{
	assert(env != NULL);
	assert(_store != NULL);

	fio_kv_store_t *store = (fio_kv_store_t *)malloc(sizeof(fio_kv_store_t));
	store->fd = env->GetIntField(_store, _store_field_fd);
	store->kv = env->GetLongField(_store, _store_field_kv);
	return store;
}

/**
 * Re-populates the fields of a Store object from a a fio_kv_store_t structure.
 */
void __fio_kv_store_set_jobject(JNIEnv *env, fio_kv_store_t *store,
		jobject _store)
{
	assert(env != NULL);
	assert(store != NULL);
	assert(_store != NULL);

	env->SetIntField(_store, _store_field_fd, store->fd);
	env->SetLongField(_store, _store_field_kv, store->kv);
}

/**
 * Converts a nvm_kv_store_info_t structure into a StoreInfo object.
 */
jobject __nvm_kv_store_info_to_jobject(JNIEnv *env, nvm_kv_store_info_t *info)
{
	assert(env != NULL);
	assert(info != NULL);

	return env->NewObject(_storeinfo_cls, _storeinfo_cstr,
			info->version, info->num_pools, info->max_pools,
			info->expiry_mode, info->num_keys, info->free_space);
}

/**
 * Convert a Pool object into a fio_kv_pool_t structure.
 *
 * A new fio_kv_pool_t structure is allocated and populated. It will need to be
 * freed by the caller when it is no longer needed.
 */
fio_kv_pool_t *__jobject_to_fio_kv_pool(JNIEnv *env, jobject _pool)
{
	assert(env != NULL);
	assert(_pool != NULL);

	fio_kv_pool_t *pool = (fio_kv_pool_t *)malloc(sizeof(fio_kv_pool_t));
	pool->store = __jobject_to_fio_kv_store(env,
			env->GetObjectField(_pool, _pool_field_store));
	pool->id = env->GetIntField(_pool, _pool_field_id);
	return pool;
}

/**
 * Convert a Key object into a fio_kv_key_t structure.
 *
 * A new fio_kv_key_t structure is allocated and populated. It will need to be
 * freed by the caller when it is no longer needed.
 */
fio_kv_key_t *__jobject_to_fio_kv_key(JNIEnv *env, jobject _key)
{
	assert(env != NULL);
	assert(_key != NULL);

	fio_kv_key_t *key = (fio_kv_key_t *)malloc(sizeof(fio_kv_key_t));
	key->length = env->GetIntField(_key, _key_field_length);
	key->bytes = (nvm_kv_key_t *)env->GetDirectBufferAddress(
			env->GetObjectField(_key, _key_field_bytes));
	return key;
}

/**
 * Convert a KeyValueInfo object into a nvm_kv_key_info_t structure.
 *
 * A new nvm_kv_key_info_t structure is allocated and populated. It will need to be
 * freed by the caller when it is no longer needed.
 */
nvm_kv_key_info_t *__jobject_to_nvm_kv_key_info(JNIEnv *env, jobject _kvinfo)
{
	assert(env != NULL);
	assert(_kvinfo != NULL);

	nvm_kv_key_info_t *kvinfo = (nvm_kv_key_info_t *)malloc(sizeof(nvm_kv_key_info_t));
	kvinfo->pool_id = env->GetIntField(_kvinfo, _kvinfo_field_pool_id);
	kvinfo->key_len = env->GetIntField(_kvinfo, _kvinfo_field_key_len);
	kvinfo->value_len = env->GetIntField(_kvinfo, _kvinfo_field_value_len);
	kvinfo->expiry = env->GetIntField(_kvinfo, _kvinfo_field_expiry);
	kvinfo->gen_count = env->GetIntField(_kvinfo, _kvinfo_field_gen_count);
	return kvinfo;
}

/**
 * Convert a nvm_kv_key_info_t structure back into a KeyValueInfo object.
 */
jobject __nvm_kv_key_info_to_jobject(JNIEnv *env, nvm_kv_key_info_t *info)
{
	assert(env != NULL);
	assert(info != NULL);

	return env->NewObject(_kvinfo_cls, _kvinfo_cstr,
			info->pool_id, info->key_len, info->value_len,
			info->expiry, info->gen_count);
}

/**
 * Re-populates the fields of a KeyValueInfo object from a a nvm_kv_key_info_t
 * structure.
 */
void __kv_key_info_set_jobject(JNIEnv *env, nvm_kv_key_info_t *info, jobject _info)
{
	assert(env != NULL);
	assert(info != NULL);
	assert(_info != NULL);

	env->SetIntField(_info, _kvinfo_field_pool_id, info->pool_id);
	env->SetIntField(_info, _kvinfo_field_key_len, info->key_len);
	env->SetIntField(_info, _kvinfo_field_value_len, info->value_len);
	env->SetIntField(_info, _kvinfo_field_expiry, info->expiry);
	env->SetIntField(_info, _kvinfo_field_gen_count, info->gen_count);
}

/**
 * Convert a Value object into a fio_kv_value_t structure.
 *
 * A new fio_kv_value_t structure is allocated and populated. It will need to
 * be freed by the caller when it is no longer needed.
 */
fio_kv_value_t *__jobject_to_fio_kv_value(JNIEnv *env, jobject _value)
{
	assert(env != NULL);
	assert(_value != NULL);

	fio_kv_value_t *value = (fio_kv_value_t *)malloc(sizeof(fio_kv_value_t));
	value->data = env->GetDirectBufferAddress(
			env->GetObjectField(_value, _value_field_data));
	value->info = __jobject_to_nvm_kv_key_info(env,
			env->GetObjectField(_value, _value_field_info));
	return value;
}

/**
 * boolean fio_kv_open(Store store)
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1open(
		JNIEnv *env, jclass cls, jobject _store)
{
	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	store->path = env->GetStringUTFChars(
			(jstring)env->GetObjectField(_store, _store_field_path), 0);
	bool ret = fio_kv_open(store);
	env->ReleaseStringUTFChars(
			(jstring)env->GetObjectField(_store, _store_field_path),
			store->path);
	__fio_kv_store_set_jobject(env, store, _store);
	free(store);
	return (jboolean)ret;
}

/**
 * void fio_kv_close(Store store);
 */
JNIEXPORT void JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1close(
		JNIEnv *env, jclass cls, jobject _store)
{
	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	fio_kv_close(store);
	__fio_kv_store_set_jobject(env, store, _store);
	free(store);
}

/**
 * bool fio_kv_destroy(Store store);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1destroy(
		JNIEnv *env, jclass cls, jobject _store)
{
	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	bool ret = fio_kv_destroy(store);
	__fio_kv_store_set_jobject(env, store, _store);
	free(store);
	return (jboolean)ret;
}

/**
 * StoreInfo fio_kv_get_store_info(Store store);
 */
JNIEXPORT jobject JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1get_1store_1info
	(JNIEnv *env, jclass cls, jobject _store)
{
	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	nvm_kv_store_info_t *info = fio_kv_get_store_info(store);
	jobject _info = info ? __nvm_kv_store_info_to_jobject(env, info) : NULL;
	free(store);
	free(info);
	return _info;
}

/**
 * Pool fio_kv_create_pool(Store store);
 */
JNIEXPORT jobject JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1create_1pool
	(JNIEnv *env, jclass cls, jobject _store, jstring _tag)
{
	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);

	const char *tag = env->GetStringUTFChars(_tag, 0);
	fio_kv_pool_t *pool = fio_kv_create_pool(store, (nvm_kv_pool_tag_t *)tag);
	env->ReleaseStringUTFChars(_tag, tag);
	jobject _pool = pool ? env->NewObject(_pool_cls, _pool_cstr, _store, pool->id, _tag) : NULL;
	free(store);
	free(pool);
	return _pool;
}

/**
 * ByteBuffer fio_kv_alloc(int length);
 */
JNIEXPORT jobject JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1alloc(
		JNIEnv *env, jclass cls, jint _length)
{
	void *p = fio_kv_alloc((uint32_t)_length);
	return p ? env->NewGlobalRef(env->NewDirectByteBuffer(p, _length)) : NULL;
}

/**
 * void fio_kv_free_value(Value value);
 */
JNIEXPORT void JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1free_1value
  (JNIEnv *env, jclass cls, jobject _value)
{
	fio_kv_value_t *value = __jobject_to_fio_kv_value(env, _value);
	fio_kv_free_value(value);
	env->DeleteGlobalRef(_value);
	env->SetObjectField(_value, _value_field_data, NULL);
	free(value->info);
	free(value);
}

/*
 * int fio_kv_get_value_len(Pool pool, Key key);
 */
JNIEXPORT jint JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1get_1value_1len
  (JNIEnv *env, jclass cls, jobject _pool, jobject _key)
{
	fio_kv_pool_t *pool;
	fio_kv_key_t *key;
	int ret;

	assert(env != NULL);
	assert(_pool != NULL);
	assert(_key != NULL);

	pool = __jobject_to_fio_kv_pool(env, _pool);
	key = __jobject_to_fio_kv_key(env, _key);

	ret = fio_kv_get_value_len(pool, key);

	free(pool->store);
	free(pool);
	free(key);
	return (jint)ret;
}

/**
 * Call a fio_kv_ operation that requires both a key structure and a value
 * structure to be passed as arguments.
 *
 * This is a simple wrapper around a fio_kv_ function that manages the
 * conversion of the JNI objects into C structures, calls the fio_kv_
 * operation, frees the structures and returns the result of the operation.
 */
int __fio_kv_call_kv(JNIEnv *env, jobject _pool, jobject _key, jobject _value,
		int (*op)(const fio_kv_pool_t *, const fio_kv_key_t *,
			const fio_kv_value_t *))
{
	fio_kv_pool_t *pool;
	fio_kv_key_t *key;
	fio_kv_value_t *value;
	int ret;

	assert(env != NULL);
	assert(_pool != NULL);
	assert(_key != NULL);
	assert(_value != NULL);
	assert(op != NULL);

	pool = __jobject_to_fio_kv_pool(env, _pool);
	key = __jobject_to_fio_kv_key(env, _key);
	value = __jobject_to_fio_kv_value(env, _value);

	// Call the operation and reset the value info fields.
	ret = op(pool, key, value);
	__kv_key_info_set_jobject(env, value->info,
			env->GetObjectField(_value, _value_field_info));

	free(pool->store);
	free(pool);
	free(key);
	free(value->info);
	free(value);
	return ret;
}

/**
 * int fio_kv_get(Pool pool, Key key, Value value);
 */
JNIEXPORT jint JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1get
  (JNIEnv *env, jclass cls, jobject _pool, jobject _key, jobject _value)
{
	return (jint)__fio_kv_call_kv(env, _pool, _key, _value, fio_kv_get);
}

/**
 * int fio_kv_put(Pool pool, Key key, Value value);
 */
JNIEXPORT jint JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1put
  (JNIEnv *env, jclass cls, jobject _pool, jobject _key, jobject _value)
{
	return (jint)__fio_kv_call_kv(env, _pool, _key, _value, fio_kv_put);
}

/**
 * int fio_kv_exists(Pool pool, Key key, KeyValueInfo info);
 */
JNIEXPORT jint JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1exists
  (JNIEnv *env, jclass cls, jobject _pool, jobject _key, jobject _info)
{
	fio_kv_pool_t *pool;
	fio_kv_key_t *key;
	nvm_kv_key_info_t *info;
	int ret;

	assert(env != NULL);
	assert(_pool != NULL);
	assert(_key != NULL);

	pool = __jobject_to_fio_kv_pool(env, _pool);
	key = __jobject_to_fio_kv_key(env, _key);
	info = _info != NULL
		? __jobject_to_nvm_kv_key_info(env, _info)
		: NULL;

	ret = fio_kv_exists(pool, key, info);

	free(pool->store);
	free(pool);
	free(key);
	free(info);
	return (jint)ret;
}

/**
 * boolean fio_kv_delete(Pool pool, Key key);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1delete
  (JNIEnv *env, jclass cls, jobject _pool, jobject _key)
{
	fio_kv_pool_t *pool;
	fio_kv_key_t *key;
	bool ret;

	assert(env != NULL);
	assert(_pool != NULL);
	assert(_key != NULL);

	pool = __jobject_to_fio_kv_pool(env, _pool);
	key = __jobject_to_fio_kv_key(env, _key);

	ret = fio_kv_delete(pool, key);

	free(pool->store);
	free(pool);
	free(key);
	return (jboolean)ret;
}

/**
 * boolean fio_kv_batch_put(Pool pool, Key[] keys, Value[] values);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1batch_1put
  (JNIEnv *env, jclass cls, jobject _pool, jobjectArray _keys, jobjectArray _values)
{
	int count;
	bool ret;

	assert(env != NULL);
	assert(_pool != NULL);
	assert(_keys != NULL);
	assert(_values != NULL);

	count = env->GetArrayLength(_keys);

	fio_kv_pool_t *pool = __jobject_to_fio_kv_pool(env, _pool);
	fio_kv_key_t **keys = (fio_kv_key_t **)calloc(count, sizeof(fio_kv_key_t *));
	fio_kv_value_t **values = (fio_kv_value_t **)calloc(count, sizeof(fio_kv_value_t *));

	for (int i=0; i<count; i++) {
		keys[i] = __jobject_to_fio_kv_key(env,
				env->GetObjectArrayElement(_keys, i));
		values[i] = __jobject_to_fio_kv_value(env,
				env->GetObjectArrayElement(_values, i));
	}

	// Call the batch operation on the sets of keys and values.
	ret = fio_kv_batch_put(pool,
			(const fio_kv_key_t **)keys,
			(const fio_kv_value_t **)values,
			count);

	for (int i=0; i<count; i++) {
		__kv_key_info_set_jobject(env, values[i]->info,
				env->GetObjectField(
					env->GetObjectArrayElement(_values, i),
					_value_field_info));

		free(keys[i]);
		free(values[i]->info);
		free(values[i]);
	}

	free(pool->store);
	free(pool);
	free(keys);
	free(values);
	return (jboolean)ret;
}

/**
 * int fio_kv_iterator(Pool pool);
 */
JNIEXPORT jint JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1iterator
  (JNIEnv *env, jclass cls, jobject _pool)
{
	fio_kv_pool_t *pool = __jobject_to_fio_kv_pool(env, _pool);
	int iterator = fio_kv_iterator(pool);
	free(pool->store);
	free(pool);
	return (jint)iterator;
}

/**
 * boolean fio_kv_next(Pool pool, int iterator);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1next
  (JNIEnv *env, jclass cls, jobject _pool, jint _iterator)
{
	fio_kv_pool_t *pool = __jobject_to_fio_kv_pool(env, _pool);
	bool ret = fio_kv_next(pool, (int)_iterator);
	free(pool->store);
	free(pool);
	return (jboolean)ret;
}

/**
 * boolean fio_kv_get_current(Pool pool, int iterator, Key key, Value value);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1get_1current
  (JNIEnv *env, jclass cls, jobject _pool, jint _iterator, jobject _key, jobject _value)
{
	fio_kv_pool_t *pool = __jobject_to_fio_kv_pool(env, _pool);
	fio_kv_key_t *key = __jobject_to_fio_kv_key(env, _key);
	fio_kv_value_t *value = __jobject_to_fio_kv_value(env, _value);

	bool ret = fio_kv_get_current(pool, (int)_iterator, key, value);

	env->SetIntField(_key, _key_field_length, key->length);
	__kv_key_info_set_jobject(env, value->info,
		env->GetObjectField(_value, _value_field_info));

	free(pool->store);
	free(pool);
	free(key);
	free(value->info);
	free(value);
	return (jboolean)ret;
}

/**
 * boolean fio_kv_end_iteration(Pool pool, int iterator);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1end_1iteration
  (JNIEnv *env, jclass cls, jobject _pool, jint _iterator)
{
	fio_kv_pool_t *pool = __jobject_to_fio_kv_pool(env, _pool);
	bool ret = fio_kv_end_iteration(pool, (int)_iterator);
	free(pool->store);
	free(pool);
	return (jboolean)ret;
}

/**
 * int fio_kv_get_last_error();
 */
JNIEXPORT jint JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1get_1last_1error
  (JNIEnv *env, jclass cls)
{
	return (jint)fio_kv_get_last_error();
}

#endif /* __FIO_KV_HELPER_USE_JNI__ */
