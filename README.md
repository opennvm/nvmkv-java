Java binding for FusionIO's key/value store API
===============================================

Description
-----------

`nvmkv-java` is a Java language binding for FusionIO's key/value store API for
FusionIO flash devices. FusionIO (http://www.fusionio.com) commercializes
PCI-Express based flash storage devices such as the ioDrive; these devices can
work in two modes, either emulating a block device and putting a file-system on
them, or as a native flash device through the use of a FusionIO-provided API.

One of these APIs, the KV API, exposes a FusionIO device as a flash-based
key/value store with atomic operations and performance levels very close to
that of the raw device.

Having access to such an API makes the FusionIO device a very interesting
storage solution that can greatly reduce the complexity of the application code
while offering orders of magnitude more performance than traditional SSD
drives. Until now, FusionIO had only been providing their API as a C++ library.
This project aims at offering a Java language binding for this library so that
applications developed in Java can benefit from the use of the native key/value
store API.


Architecture
------------

The `nvmkv-java` library is made of two distinct components:

* A helper library, `libfio_kv_helper`, written in C, that wraps around
  FusionIO's KV API while offering a more sensible interface that is easier to
  manipulate and to expose in the Java world via JNI.
* A Java library, `nvmkv-java-api` that exposes the native calls to the helper
  library in a Java-esque way, with iterators, helper functions, etc.


License
-------

This Java binding to FusionIO's NVMKV API is released as open-source software
under the terms of the BSD 3-clause license. See `COPYING` for more
information.


Dependencies
------------

`nvmkv-java`'s helper library depends on FusionIO's `vsl`, `nvmkv` and
`nvm-primitives` libraries and on the NVMKV API header files. These are _not_
provided as part of this package nor are these dependencies managed by the
Maven build.

The helper library depends on JNI (Java Native Interface), which means you need
to build the Maven project with a JDK instead of a JRE. Both `jni.h` and
`jni_md.h` must be in the JDK`s `include/` directory.


Building
--------

Each component resides in its own Maven module, and both are tied together into
the top-level `pom.xml` so you can simply run:

```
$ mvn
```

To build everything. The build outputs will be placed in
`dist/target/nvmkv-java-dist-<version>-bin/`:

* `libfio_kv_helper-<version>.so`, the C shared library
* `nvmkv-java-api-<version>.jar`, the Java binding

You can also build the API's Javadoc with:

```
$ cd api/
$ mvn javadoc:javadoc
```

And the results will be available under `target/site/apidocs/`.


Executing the test suite
------------------------

Running the tests requires an available, attached FusionIO device at
`/dev/fioa`. Note that **running the tests is destructive of all data on the
card!**

```
$ mvn test -Dtests.disable=false
```

Usage
-----

First, make sure that the Java binding JAR are on your classpath, and that your
`LD_LIBRARY_PATH` contains the directory with the
`libfio_kv_helper-<version>.so` library.

On the Java side, you can now simply access the library through a
`FusionIOAPI` instance:

```java
import com.turn.fusionio.FusionIOAPI;

// FusionIO device, usually /dev/fctX
private static final String FUSION_IO_PATH = "/dev/fct0";

/* ID of the key/value pairs pool to use on the device. 0 is the default pool
 * which doesn't require a pool creation. */
private static final int FUSION_IO_POOL_ID = 0;

// First, open the API on the desired device or directFS file.
Store fio = FusionIOAPI.get(FUSION_IO_PATH,       // Path to the device.
                            0,                    // Store version number.
                            ExpiryMode.NO_EXPIRY, // The store expiry mode.
                            0);                   // The expiry TTL, if global
                                                  // expiry is used.
fio.open();

// Second, retrieve a handle to the pool of your choice, here the default pool.
Pool pool = fio.getPool(FUSION_IO_POOL_ID);

/* Alternatively, you can also get or create pools by "tags", or pool names.
 * Tags must be 16 characters or less and are unique. The default pool has no
 * name. */
pool = fio.getOrCreatePool("profiles");
```

To write a key/value pair:

```java
/* Create a key.
 *
 * Key.createFrom(long); is a convenient way of creating keys from long integer
 * values. But keys can be anything up to 128 bytes long. You can also
 * simply create a Key from an existing byte[] with Key.createFrom(byte[]), or
 * empty for you to fill in with Key.get(size). */
Key key = Key.get(64); // creates a 64-byte key
ByteBuffer keyData = key.getByteBuffer(); // put stuff in keyData

key = Key.createFrom(new byte[] { 0x66, 0x6F, 0x6F }); // 'foo'
key = Key.createFrom(42);

/* Create a value.
 *
 * Creating a value allocates sector-aligned memory into which you can
 * place the data to be stored. You can easily access this memory through
 * a ByteBuffer. */
Value value = Value.get(value_len);
ByteBuffer data = value.getByteBuffer();

// Put stuff into the data ByteBuffer...

// Write the key/value pair into the pool.
pool.put(key, value);

// Make sure you free the allocated value memory as this is not memory managed
// by the JVM!
value.free();
value = null;
```

To read a value (you need to know the value length in advance with the current
version of FusionIO's KV library):

```java
Value readback = new Value(value_len);
pool.get(Key.createFrom(42L), readback);
ByteBuffer data = readback.getByteBuffer();

// Do stuff with the data ByteBuffer...

// Always free the value once you're done with it!
value.free();
value = null;
```

Batch operations are supported through the same `get`, `put` and `remove`
methods and seamlessly operate on arrays of `Key`s and `Value`s. Check out
`com.turn.fusionio.FusionIOAPI` for the full specification of the available
methods.


Performance considerations
--------------------------

Because the data contained in a `Key` or in a `Value` is allocated from the
native side, it is strongly recommended that users of the `FusionIOAPI` limit
the allocation of new keys or values to a minimum.

If your keys all have the same size it is much faster to use a single `Key`
object and change its content rather than reallocating a new one. For values,
you usually have to allocate a `Value` object of the maximum possible size
anyway, so here again it is recommended to use a single `Value` object and
re-use its allocated buffer as much as possible.


Authors
-------

* Maxime Petazzoni <mpetazzoni@turn.com> (Sr. Platform Engineer at Turn Inc)
  Original author, main developer and maintainer.
* FusionIO's SDK team, for their answers, reviews and feedback.


Caveats
-------

* Value memory buffers need to be sector-aligned. This imposes memory
  allocations to be performed at the user's request on the native side and then
  exposed as a ByteBuffer to the user, instead of being able to directly read
  from a user-provided ByteBuffer.

* Because of the requirement for sector-aligned memory, this memory is
  allocated from the native side and thus cannot be managed by the JVM's
  garbage collector. It is thus the responsibility of the user to correctly
  free this memory by calling `free()` on a `Value` object before loosing the
  reference to it. This is the only way by which the memory used by these
  sector-aligned buffers can be recovered.

* Expiry modes are currently not supported by this binding (global expiry isn't
  yet supported by directKV anyway).
