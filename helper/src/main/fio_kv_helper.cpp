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
 *	path (const char *): Path to the Fusion-IO device file.
 *	pool_id (int): The ID of the pool to use of the device (when not using
 *	  directFS mode). Should be 0 either way until pools are properly supported
 *	  by this library.
 * Returns:
 *	Returns a fio_kv_store_t structure containing the file descriptor
 *	associated with the open device and the KV store ID used by the KV SDK
 *	API.
 */
fio_kv_store_t *fio_kv_open(const char *path, const int pool_id)
{
	fio_kv_store_t *store;
	int max_pools = FIO_KV_MAX_POOLS;
	int flags = O_RDWR | O_DIRECT;

	assert(path != NULL);
	assert(pool_id >= 0);

	if (strncmp(path, "/dev", 4) == 0) {
		assert(pool_id == 0);
		max_pools = 0;
		flags |= O_LARGEFILE;
	}

	store	= (fio_kv_store_t *)calloc(1, sizeof(fio_kv_store_t));

	store->fd = open(path, flags);
	if (store->fd < 0) {
		free(store);
		return NULL;
	}

	store->kv = nvm_kv_create(store->fd, FIO_KV_API_VERSION, max_pools,
			KV_DISABLE_EXPIRY, false);
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
 * Retrieve the value associated with a given key in a key/value store.
 *
 * Note that you must provide sector-aligned memory that will can contain the
 * requested number of bytes. Such memory can be obtained by calling
 * fio_kv_alloc().
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 *	key (fio_kv_key_t *): The key.
 *	value (fio_kv_value_t *): An allocated value structure to hold the
 *	  result.
 * Returns:
 *	The number of bytes read, or -1 if the read failed.
 */
int fio_kv_get(const fio_kv_store_t *store, const fio_kv_key_t *key,
		const fio_kv_value_t *value)
{
	assert(store != NULL);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);
	assert(value != NULL);
	assert(value->data != NULL);
	assert(value->info != NULL);

	return nvm_kv_get(store->kv, store->pool,
			key->bytes, key->length,
			value->data, value->info->value_len,
			value->info);
}

/**
 * Insert (or replace) a key/value pair into the store.
 *
 * Note that the value's data must be sector-aligned memory, which can be
 * obtained from the fio_kv_alloc() function.
 *
 * Args:
 *	store (fio_kv_store_t *): A pointer to a opened KV store.
 *	key (fio_kv_key_t): The key.
 *	value (fio_kv_value_t): The value.
 * Returns:
 *	The number of bytes written, or -1 in case of an error.
 */
int fio_kv_put(const fio_kv_store_t *store, const fio_kv_key_t *key,
		const fio_kv_value_t *value)
{
	assert(store != NULL);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);
	assert(value != NULL);
	assert(value->data != NULL);
	assert(value->info != NULL);

	return nvm_kv_put(store->kv, store->pool,
			key->bytes, key->length,
			value->data, value->info->value_len,
			value->info->expiry, true, 0);
}

/**
 * Tell whether a specific key exists in a key/value store.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 *	key (fio_kv_key_t *): The key of the mapping to check the existence of.
 *	info (nvm_kv_key_info_t *): An optional pointer to a nvm_kv_key_info_t
 *	  structure to fill with key/value pair information if it exists in the
 *	  key/value store.
 * Returns:
 *	Returns 1 if a mapping for the given key exists in the key/value store,
 *	0 if it doesn't or -1 if an error occured.
 */
int fio_kv_exists(const fio_kv_store_t *store, const fio_kv_key_t *key,
		nvm_kv_key_info_t *info)
{
	assert(store != NULL);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);

	// TODO(mpetazzoni): remove once nvm_kv_exists() no longer segfaults when
	// passed a NULL nvm_kv_key_info_t parameter.
	nvm_kv_key_info_t _info;
	if (info == NULL) {
		info = &_info;
	}

	return nvm_kv_exists(store->kv, store->pool, key->bytes, key->length, info);
}

/**
 * Remove a key/value pair from the key/value store.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 *	key (fio_kv_key_t *): The key to the mapping to remove.
 * Returns:
 *	Returns true if the mapping was successfuly removed, false otherwise.
 */
bool fio_kv_delete(const fio_kv_store_t *store, const fio_kv_key_t *key)
{
	assert(store != NULL);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);

	return nvm_kv_delete(store->kv, store->pool, key->bytes, key->length) == 0;
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
 * Retrieve a set of values corresponding to the given set of keys in one
 * batch operation.
 *
 * Note that you must provide sector-aligned memory of the appropriate size for
 * each value.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 *	keys (fio_kv_key_t **): An array of pointer to the keys to retrieve.
 *	values (fio_kv_value_t **): An array of pointers to allocated value
 *	  structures to hold the results.
 *	count (size_t): The number of key/value pairs.
 * Returns:
 *	Returns true if the batch retrieval was successful, false otherwise.
 */
bool fio_kv_batch_get(const fio_kv_store_t *store, const fio_kv_key_t **keys,
		const fio_kv_value_t **values, const size_t count)
{
	nvm_kv_iovec_t *v;
	int ret;

	assert(store != NULL);
	assert(keys != NULL);
	assert(values != NULL);

	v = _fio_kv_prepare_batch(keys, values, count);
	ret = nvm_kv_batch_get(store->kv, store->pool, v, count);
	free(v);

	return ret == 0;
}

/**
 * Insert (or replace) a set of key/value pairs in one batch operation.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 *	keys (fio_kv_key_t **): An array of pointers to the keys to insert.
 *	values (fio_kv_value_t **): An array of pointers to the corresponding
 *	  values.
 *	count (size_t): The number of key/value pairs.
 * Returns:
 *	Returns true if the batch insertion was successful, false otherwise.
 */
bool fio_kv_batch_put(const fio_kv_store_t *store, const fio_kv_key_t **keys,
		const fio_kv_value_t **values, const size_t count)
{
	nvm_kv_iovec_t *v;
	int ret;

	assert(store != NULL);
	assert(keys != NULL);
	assert(values != NULL);

	v = _fio_kv_prepare_batch(keys, values, count);
	ret = nvm_kv_batch_put(store->kv, store->pool, v, count);
	free(v);

	return ret == 0;
}

/**
 * Remove a set of key/value pairs in one batch operation.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 *	keys (fio_kv_key_t **): An array of pointers to the keys to remove.
 *	count (size_t): The number of key/value pairs.
 * Returns:
 *	Returns true if the batch insertion was successful, false otherwise.
 */
bool fio_kv_batch_delete(const fio_kv_store_t *store,
		const fio_kv_key_t **keys, const size_t count)
{
	nvm_kv_iovec_t *v;
	int ret;

	assert(store != NULL);
	assert(keys != NULL);

	v = _fio_kv_prepare_batch(keys, NULL, count);
	ret = nvm_kv_batch_delete(store->kv, store->pool, v, count);
	free(v);

	return ret == 0;
}

/**
 * Create an iterator on a key/value store.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 * Returns:
 *	Returns the new iterator's ID, or -1 if an iterator could not be
 *	created.
 */
int fio_kv_iterator(const fio_kv_store_t *store)
{
	assert(store != NULL);

	return nvm_kv_begin(store->kv, store->pool);
}

/**
 * Move an iterator to the following element in a key/value store.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 *	iterator (int): The ID of the iterator to advance.
 * Returns:
 *	Returns true if the operation was successful, false otherwise.
 */
bool fio_kv_next(const fio_kv_store_t *store, const int iterator)
{
	assert(store != NULL);
	assert(iterator >= 0);

	return nvm_kv_next(store->kv, iterator) == 0;
}

/**
 * Retrieve the key and value at the current iterator's position.
 *
 * Args:
 *	store (fio_kv_store_t *): The key/value store.
 *	iterator (int): The ID of the iterator.
 *	key (fio_kv_key_t *): An allocated key structure to hold the current
 *	  key.
 *	value (fio_kv_value_t *): An allocated value structure to hold the
 *	  current value.
 * Returns:
 *	Returns true if the operation was successful, false otherwise.
 */
bool fio_kv_get_current(const fio_kv_store_t *store, const int iterator,
		fio_kv_key_t *key, const fio_kv_value_t *value)
{
	assert(store != NULL);
	assert(iterator >= 0);
	assert(key != NULL);
	assert(key->length >= 1 && key->length <= NVM_KV_MAX_KEY_SIZE);
	assert(key->bytes != NULL);
	assert(value != NULL);
	assert(value->data != NULL);
	assert(value->info != NULL);

	return nvm_kv_get_current(store->kv, iterator,
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
#include "com_turn_fusionio_FusionIOAPI_HelperLibrary.h"

static jclass _store_cls;
static jmethodID _store_cstr;
static jfieldID _store_field_fd,
								_store_field_kv,
								_store_field_pool;

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
JNIEXPORT void JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1init_1jni_1cache
  (JNIEnv *env, jclass cls)
{
	jclass _cls;

	_cls = env->FindClass("com/turn/fusionio/Store");
	assert(_cls != NULL);
	_store_cls = (jclass) env->NewGlobalRef(_cls);
	env->DeleteLocalRef(_cls);
	_store_cstr = env->GetMethodID(_store_cls, "<init>", "(IJI)V");
	_store_field_fd   = env->GetFieldID(_store_cls, "fd",   "I");
	_store_field_kv   = env->GetFieldID(_store_cls, "kv",   "J");
	_store_field_pool = env->GetFieldID(_store_cls, "pool", "I");

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
	store->pool = env->GetIntField(_store, _store_field_pool);
	return store;
}

/**
 * Convert a fio_kv_store_t structure back into a Store object.
 */
jobject __fio_kv_store_to_jobject(JNIEnv *env, fio_kv_store_t *store)
{
	assert(env != NULL);
	assert(store != NULL);

	return env->NewObject(_store_cls, _store_cstr,
			store->fd, store->kv, store->pool);
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
	env->SetIntField(_store, _store_field_pool, store->pool);
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
 * Convert a fio_kv_key_t structure back into a Key object.
 */
jobject __fio_kv_key_to_jobject(JNIEnv *env, fio_kv_key_t *key)
{
	assert(env != NULL);
	assert(key != NULL);

	jobject _key = env->NewObject(_key_cls, _key_cstr);
	env->SetIntField(_key, _key_field_length, key->length);
	env->SetObjectField(_key, _key_field_bytes,
			env->NewDirectByteBuffer(key->bytes, key->length));
	return _key;
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
 * Converts a fio_kv_value_t structure back into a Value object.
 */
jobject __fio_kv_value_to_jobject(JNIEnv *env, fio_kv_value_t *value)
{
	assert(env != NULL);
	assert(value != NULL);

	jobject _value = env->NewObject(_value_cls, _value_cstr);
	env->SetObjectField(_value, _value_field_data,
			env->NewDirectByteBuffer(value->data, value->info->value_len));
	env->SetObjectField(_value, _value_field_info,
			__nvm_kv_key_info_to_jobject(env, value->info));
	return _value;
}

/**
 * Store fio_kv_open(String device, int pool_id);
 */
JNIEXPORT jobject JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1open(
		JNIEnv *env, jclass cls, jstring _device, jint _pool_id)
{
	const char *device = env->GetStringUTFChars(_device, 0);
	fio_kv_store_t *store = fio_kv_open(device, (int)_pool_id);
	env->ReleaseStringUTFChars(_device, device);

	jobject _store = store ? __fio_kv_store_to_jobject(env, store) : NULL;
	free(store);
	return _store;
}

/**
 * void fio_kv_close(Store store);
 */
JNIEXPORT void JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1close(
		JNIEnv *env, jclass cls, jobject _store)
{
	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	fio_kv_close(store);
	__fio_kv_store_set_jobject(env, store, _store);
	free(store);
}

/**
 * ByteBuffer fio_kv_alloc(int length);
 */
JNIEXPORT jobject JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1alloc(
		JNIEnv *env, jclass cls, jint _length)
{
	void *p = fio_kv_alloc((uint32_t)_length);
	return p ? env->NewDirectByteBuffer(p, _length) : NULL;
}

/**
 * void fio_kv_free_value(Value value);
 */
JNIEXPORT void JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1free_1value
  (JNIEnv *env, jclass cls, jobject _value)
{
	fio_kv_value_t *value = __jobject_to_fio_kv_value(env, _value);
	fio_kv_free_value(value);
	env->SetObjectField(_value, _value_field_data, NULL);
	free(value->info);
	free(value);
}

/**
 * Call a fio_kv_ operation that requires both a key structure and a value
 * structure to be passed as arguments.
 *
 * This is a simple wrapper around a fio_kv_ function that manages the
 * conversion of the JNI objects into C structures, calls the fio_kv_
 * operation, frees the structures and returns the result of the operation.
 */
int __fio_kv_call_kv(JNIEnv *env, jobject _store, jobject _key, jobject _value,
		int (*op)(const fio_kv_store_t *, const fio_kv_key_t *,
			const fio_kv_value_t *))
{
	fio_kv_store_t *store;
	fio_kv_key_t *key;
	fio_kv_value_t *value;
	int ret;

	assert(env != NULL);
	assert(_store != NULL);
	assert(_key != NULL);
	assert(_value != NULL);
	assert(op != NULL);

	store = __jobject_to_fio_kv_store(env, _store);
	key = __jobject_to_fio_kv_key(env, _key);
	value = __jobject_to_fio_kv_value(env, _value);

	// Call the operation and reset the value info fields.
	ret = op(store, key, value);
	__kv_key_info_set_jobject(env, value->info,
			env->GetObjectField(_value, _value_field_info));

	free(store);
	free(key);
	free(value->info);
	free(value);
	return ret;
}

/**
 * Call a fio_kv_ operation that requires a key structure to be passed as the
 * argument.
 *
 * This is a simple wrapper around a fio_kv_ function that manages the
 * conversion of the JNI objects into C structures, calls the fio_kv_
 * operation, frees the structures and returns the result of the operation.
 */
bool __fio_kv_call_k(JNIEnv *env, jobject _store, jobject _key,
		bool (*op)(const fio_kv_store_t *, const fio_kv_key_t *))
{
}

/**
 * int fio_kv_get(Store store, Key key, Value value);
 */
JNIEXPORT jint JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1get
  (JNIEnv *env, jclass cls, jobject _store, jobject _key, jobject _value)
{
	return (jint)__fio_kv_call_kv(env, _store, _key, _value, fio_kv_get);
}

/**
 * int fio_kv_put(Store store, Key key, Value value);
 */
JNIEXPORT jint JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1put
  (JNIEnv *env, jclass cls, jobject _store, jobject _key, jobject _value)
{
	return (jint)__fio_kv_call_kv(env, _store, _key, _value, fio_kv_put);
}

/**
 * int fio_kv_exists(Store store, Key key, KeyValueInfo info);
 */
JNIEXPORT jint JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1exists
  (JNIEnv *env, jclass cls, jobject _store, jobject _key, jobject _info)
{
	fio_kv_store_t *store;
	fio_kv_key_t *key;
	nvm_kv_key_info_t *info;
	int ret;

	assert(env != NULL);
	assert(_store != NULL);
	assert(_key != NULL);

	store = __jobject_to_fio_kv_store(env, _store);
	key = __jobject_to_fio_kv_key(env, _key);
	info = _info != NULL
		? __jobject_to_nvm_kv_key_info(env, _info)
		: NULL;

	ret = fio_kv_exists(store, key, info);

	free(store);
	free(key);
	free(info);
	return (jint)ret;
}

/**
 * boolean fio_kv_delete(Store store, Key key);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1delete
  (JNIEnv *env, jclass cls, jobject _store, jobject _key)
{
	fio_kv_store_t *store;
	fio_kv_key_t *key;
	bool ret;

	assert(env != NULL);
	assert(_store != NULL);
	assert(_key != NULL);

	store = __jobject_to_fio_kv_store(env, _store);
	key = __jobject_to_fio_kv_key(env, _key);

	ret = fio_kv_delete(store, key);

	free(store);
	free(key);
	return (jboolean)ret;
}

/**
 * Call a fio_kv_batch_ operation that requires both arrays of keys and values
 * to be passed as arguments.
 *
 * This is a simple wrapper around a fio_kv_batch function that manages the
 * conversion of the JNI objects into C structures, calls the fio_kv_batch_
 * operation, frees the structures and returns the result of the operation.
 */
bool __fio_kv_batch_call_kv(JNIEnv *env, jobject _store, jobjectArray _keys,
		jobjectArray _values,
		bool (*op)(const fio_kv_store_t *, const fio_kv_key_t **,
			const fio_kv_value_t **, const size_t))
{
	int count;
	bool ret;

	assert(env != NULL);
	assert(_store != NULL);
	assert(_keys != NULL);
	assert(_values != NULL);
	assert(op != NULL);

	count = env->GetArrayLength(_keys);

	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	fio_kv_key_t **keys = (fio_kv_key_t **)calloc(count, sizeof(fio_kv_key_t *));
	fio_kv_value_t **values = (fio_kv_value_t **)calloc(count, sizeof(fio_kv_value_t *));

	for (int i=0; i<count; i++) {
		keys[i] = __jobject_to_fio_kv_key(env,
				env->GetObjectArrayElement(_keys, i));
		values[i] = __jobject_to_fio_kv_value(env,
				env->GetObjectArrayElement(_values, i));
	}

	// Call the batch operation on the sets of keys and values.
	ret = op(store, (const fio_kv_key_t **)keys, (const fio_kv_value_t **)values,
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
	free(store);
	free(keys);
	free(values);
	return ret;
}

/**
 * Call a fio_kv_batch_ operation that requires an array of keys to be passed
 * as argument.
 *
 * This is a simple wrapper around a fio_kv_batch function that manages the
 * conversion of the JNI objects into C structures, calls the fio_kv_batch_
 * operation, frees the structures and returns the result of the operation.
 */
bool __fio_kv_batch_call_k(JNIEnv *env, jobject _store, jobjectArray _keys,
		bool (*op)(const fio_kv_store_t *, const fio_kv_key_t **, const size_t))
{
	int count;
	bool ret;

	assert(env != NULL);
	assert(_store != NULL);
	assert(_keys != NULL);
	assert(op != NULL);

	count = env->GetArrayLength(_keys);

	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	fio_kv_key_t **keys = (fio_kv_key_t **)calloc(count, sizeof(fio_kv_key_t *));

	for (int i=0; i<count; i++) {
		keys[i] = __jobject_to_fio_kv_key(env,
				env->GetObjectArrayElement(_keys, i));
	}

	// Call the batch operation on the set of keys.
	ret = op(store, (const fio_kv_key_t **)keys, count);

	for (int i=0; i<count; i++) {
		free(keys[i]);
	}
	free(store);
	free(keys);
	return ret;
}

/**
 * boolean fio_kv_batch_get(Store store, Key[] keys, Value[] values);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1batch_1get
  (JNIEnv *env, jclass cls, jobject _store, jobjectArray _keys, jobjectArray _values)
{
	return (jboolean)__fio_kv_batch_call_kv(env, _store,
			_keys, _values, fio_kv_batch_get);
}

/**
 * boolean fio_kv_batch_put(Store store, Key[] keys, Value[] values);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1batch_1put
  (JNIEnv *env, jclass cls, jobject _store, jobjectArray _keys, jobjectArray _values)
{
	return (jboolean)__fio_kv_batch_call_kv(env, _store,
			_keys, _values, fio_kv_batch_put);
}

/**
 * boolean fio_kv_batch_delete(Store store, Key[] keys);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1batch_1delete
  (JNIEnv *env, jclass cls, jobject _store, jobjectArray _keys)
{
	return (jboolean)__fio_kv_batch_call_k(env, _store,
			_keys, fio_kv_batch_delete);
}

/**
 * int fio_kv_iterator(Store store);
 */
JNIEXPORT jint JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1iterator
  (JNIEnv *env, jclass cls, jobject _store)
{
	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	int iterator = fio_kv_iterator(store);
	free(store);
	return (jint)iterator;
}

/**
 * boolean fio_kv_next(Store store, int iterator);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1next
  (JNIEnv *env, jclass cls, jobject _store, jint _iterator)
{
	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	bool ret = fio_kv_next(store, (int)_iterator);
	free(store);
	return (jboolean)ret;
}

/**
 * boolean fio_kv_get_current(Store store, int iterator, Key key, Value value);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1get_1current
  (JNIEnv *env, jclass cls, jobject _store, jint _iterator, jobject _key, jobject _value)
{
	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	fio_kv_key_t *key = __jobject_to_fio_kv_key(env, _key);
	fio_kv_value_t *value = __jobject_to_fio_kv_value(env, _value);

	bool ret = store && key && value &&
		fio_kv_get_current(store, (int)_iterator, key, value);

	__kv_key_info_set_jobject(env, value->info,
		env->GetObjectField(_value, _value_field_info));

	free(store);
	free(key);
	free(value->info);
	free(value);
	return (jboolean)ret;
}

/**
 * int fio_kv_get_last_error();
 */
JNIEXPORT jint JNICALL Java_com_turn_fusionio_FusionIOAPI_00024HelperLibrary_fio_1kv_1get_1last_1error
  (JNIEnv *env, jclass cls)
{
	return (jint)fio_kv_get_last_error();
}

#endif /* __FIO_KV_HELPER_USE_JNI__ */
