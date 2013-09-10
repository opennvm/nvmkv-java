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

#include "com_turn_fusionio_FusionIOAPI.h"
#include "fio_kv_helper.h"

static jclass _expiry_mode_cls;
static jmethodID _expiry_mode_ordinal;

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

	_cls = env->FindClass("com/turn/fusionio/FusionIOAPI$ExpiryMode");
	assert(_cls != NULL);
	_expiry_mode_cls = (jclass) env->NewGlobalRef(_cls);
	env->DeleteLocalRef(_cls);
	_expiry_mode_ordinal = env->GetMethodID(_expiry_mode_cls, "ordinal", "()I");

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
	// We don't really need to bother with the pool tag here.
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
 * boolean fio_kv_open(Store store, int version, ExpiryMode expiryMode, int expiryTime)
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1open(
		JNIEnv *env, jclass cls, jobject _store, jint _version,
		jobject _expiry_mode, jint _expiry_time)
{
	assert(_store != NULL);
	assert(_expiry_mode != NULL);

	nvm_kv_expiry_t expiry_mode = (nvm_kv_expiry_t)
		env->CallIntMethod(_expiry_mode, _expiry_mode_ordinal);

	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	store->path = env->GetStringUTFChars(
			(jstring)env->GetObjectField(_store, _store_field_path), 0);

	bool ret = fio_kv_open(store, (uint32_t)_version, expiry_mode,
			(uint32_t)_expiry_time);

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
	assert(_store != NULL);

	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	fio_kv_close(store);
	__fio_kv_store_set_jobject(env, store, _store);

	free(store);
}

/**
 * StoreInfo fio_kv_get_store_info(Store store);
 */
JNIEXPORT jobject JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1get_1store_1info
	(JNIEnv *env, jclass cls, jobject _store)
{
	assert(_store != NULL);

	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	nvm_kv_store_info_t *info = fio_kv_get_store_info(store);
	jobject _info = info ? __nvm_kv_store_info_to_jobject(env, info) : NULL;

	free(store);
	free(info);
	return _info;
}

/**
 * Pool fio_kv_get_or_create_pool(Store store);
 */
JNIEXPORT jobject JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1get_1or_1create_1pool
	(JNIEnv *env, jclass cls, jobject _store, jstring _tag)
{
	assert(_store != NULL);

	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	const char *tag = env->GetStringUTFChars(_tag, 0);

	fio_kv_pool_t *pool = fio_kv_get_or_create_pool(store, tag);
	jobject _pool = pool ? env->NewObject(_pool_cls, _pool_cstr, _store, pool->id, _tag) : NULL;

	env->ReleaseStringUTFChars(_tag, tag);
	free(store);
	free(pool);
	return _pool;
}

/**
 * Pool[] fio_kv_get_all_pools(Store store);
 */
JNIEXPORT jobjectArray JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1get_1all_1pools
	(JNIEnv *env, jclass cls, jobject _store)
{
	assert(_store != NULL);

	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	uint32_t count;
	fio_kv_pool_t *pools = fio_kv_get_all_pools(store, &count);
	if (pools == NULL) {
		free(store);
		return NULL;
	}

	jobjectArray _pools = env->NewObjectArray(count, _pool_cls, (jobject)NULL);
	for (int i = 0 ; i < count ; i++) {
		env->SetObjectArrayElement(_pools, (jsize)i,
				env->NewObject(_pool_cls, _pool_cstr,
					_store, (jint) (pools[i].id), env->NewStringUTF(pools[i].tag)));
	}

	free(store);
	free(pools);
	return _pools;
}

/**
 * boolean fio_kv_delete_pool(Pool pool);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1delete_1pool
	(JNIEnv *env, jclass cls, jobject _pool)
{
	assert(_pool != NULL);

	fio_kv_pool_t *pool = __jobject_to_fio_kv_pool(env, _pool);
	bool ret = fio_kv_delete_pool(pool);

	free(pool->store);
	free(pool);
	return (jboolean)ret;
}

/**
 * boolean fio_kv_delete_all_pool(Store store);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1delete_1all_1pools
	(JNIEnv *env, jclass cls, jobject _store)
{
	assert(_store != NULL);

	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	bool ret = fio_kv_delete_all_pools(store);

	free(store);
	return (jboolean)ret;
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
	assert(_value != NULL);

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
	assert(env != NULL);
	assert(_pool != NULL);
	assert(_key != NULL);

	fio_kv_pool_t *pool = __jobject_to_fio_kv_pool(env, _pool);
	fio_kv_key_t *key = __jobject_to_fio_kv_key(env, _key);

	int ret = fio_kv_get_value_len(pool, key);

	free(pool->store);
	free(pool);
	free(key);
	return (jint)ret;
}

/*
 * KeyValueInfo fio_kv_get_key_info(Pool pool, Key key);
 */
JNIEXPORT jobject JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1get_1key_1info
	(JNIEnv *env, jclass cls, jobject _pool, jobject _key)
{
	assert(env != NULL);
	assert(_pool != NULL);
	assert(_key != NULL);

	fio_kv_pool_t *pool = __jobject_to_fio_kv_pool(env, _pool);
	fio_kv_key_t *key = __jobject_to_fio_kv_key(env, _key);

	nvm_kv_key_info_t *info = fio_kv_get_key_info(pool, key);
	jobject _info = info != NULL
		? __nvm_kv_key_info_to_jobject(env, info)
		: NULL;

	free(pool->store);
	free(pool);
	free(key);
	free(info);
	return _info;
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
	assert(env != NULL);
	assert(_pool != NULL);
	assert(_key != NULL);
	assert(_value != NULL);
	assert(op != NULL);

	fio_kv_pool_t *pool = __jobject_to_fio_kv_pool(env, _pool);
	fio_kv_key_t *key = __jobject_to_fio_kv_key(env, _key);
	fio_kv_value_t *value = __jobject_to_fio_kv_value(env, _value);

	// Call the operation and reset the value info fields.
	int ret = op(pool, key, value);
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
 * boolean fio_kv_exists(Pool pool, Key key, KeyValueInfo info);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1exists
  (JNIEnv *env, jclass cls, jobject _pool, jobject _key, jobject _info)
{
	assert(env != NULL);
	assert(_pool != NULL);
	assert(_key != NULL);

	fio_kv_pool_t *pool = __jobject_to_fio_kv_pool(env, _pool);
	fio_kv_key_t *key = __jobject_to_fio_kv_key(env, _key);
	nvm_kv_key_info_t *info = _info != NULL
		? __jobject_to_nvm_kv_key_info(env, _info)
		: NULL;

	bool ret = fio_kv_exists(pool, key, info);

	free(pool->store);
	free(pool);
	free(key);
	free(info);
	return (jboolean)ret;
}

/**
 * boolean fio_kv_delete(Pool pool, Key key);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1delete
  (JNIEnv *env, jclass cls, jobject _pool, jobject _key)
{
	assert(env != NULL);
	assert(_pool != NULL);
	assert(_key != NULL);

	fio_kv_pool_t *pool = __jobject_to_fio_kv_pool(env, _pool);
	fio_kv_key_t *key = __jobject_to_fio_kv_key(env, _key);
	bool ret = fio_kv_delete(pool, key);

	free(pool->store);
	free(pool);
	free(key);
	return (jboolean)ret;
}

/**
 * boolean fio_kv_delete_all(Store store);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1delete_1all
  (JNIEnv *env, jclass cls, jobject _store)
{
	assert(env != NULL);
	assert(_store != NULL);

	fio_kv_store_t *store = __jobject_to_fio_kv_store(env, _store);
	bool ret = fio_kv_delete_all(store);

	free(store);
	return (jboolean)ret;
}

/**
 * boolean fio_kv_batch_put(Pool pool, Key[] keys, Value[] values);
 */
JNIEXPORT jboolean JNICALL Java_com_turn_fusionio_FusionIOAPI_fio_1kv_1batch_1put
  (JNIEnv *env, jclass cls, jobject _pool, jobjectArray _keys, jobjectArray _values)
{
	assert(env != NULL);
	assert(_pool != NULL);
	assert(_keys != NULL);
	assert(_values != NULL);

	int count = env->GetArrayLength(_keys);

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
	bool ret = fio_kv_batch_put(pool,
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
	assert(_pool != NULL);

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
	assert(_pool != NULL);

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
	assert(_pool != NULL);
	assert(_key != NULL);
	assert(_value != NULL);

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
	assert(_pool != NULL);

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
