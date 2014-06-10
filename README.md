# Memcached mod for Vert.x #

This module provides a Vert.x client for cache/storage servers, implementing memcached protocol. The name of this mod is `vertx-memcached`. 

Version 3.0.0 supports the asynchronous listeners released by spymemcached starting version 2.9. The code was totally re-factored.
Version 2.1.0 of this project is deprecated now and you are strongly advised to upgrade.  

## Dependencies ##

Memcached connectivity is implemented using a non-blocking spymemcached client. Current version is built and tested with spymemcached 2.11.3.

More info about spymemcached project is available [here](http://code.google.com/p/spymemcached/ "spymemcached"). 
  

## Configuration ##

`vertx-memcached` is a **non**-worker mod. Each `vertx-memcached` verticle can incorporate one-to-n spymemcached clients in a "pool". 

Here is an example of`vertx-memcached` worker configuration:

<pre>
<code>
{
    "address" : "vertx.memcached",
    "memcached.servers": "localhost:11211",
    "memcached.timeout.ms": 2500,
    "memcached.connections": 2
}
</code>
</pre>
 
where

- `address` - the eventbus address of mod's verticles . Mandatory.

- `validate-on-connect` - if set to true, an attempt to call memcached GETSTATS will be done on spymemcached client init, this to check if memcached servers are available. Optional, default to false.

- `memcached.servers` - the address of your memcached server(s). 1 to n space separated addresses can be passed, each of these should be in the following format: `<hostname:port>`. Mandatory.

- `memcached.connections` - the number of spymemcached clients that will be initialized on verticle start up. These clients will be used randomly, in a way that mimics a connection pool behavior. Since spymemcached client is async and non-blocking, there is no need to use a lot of clients in such pool. Optional, default - 2.

- `memcached.timeout.ms` - in case operations submitted to memcached server (see above) were not completed within number of milliseconds provided with this parameter, the operation is cancelled and time-out error is returned. Optional, default value of net.spy.memcached.DefaultConnectionFactory.DEFAULT_OPERATION_TIMEOUT value will be used (currently = 2500L)


## Usage ##

Memcached storage is done using key-value pairs, where key is always a String. The value can be of any serializable type. Keep in mind that not all types of objects can be transmitted through vert.x's eventbus though. 

## Supported memcached commands ##

** All spymemcached operations are executed using default transcoder. ** 

- `set` - Set an object in the cache regardless of any existing value
<pre>
<code>
{
	"command":"set",
	"key":"AAA",
	"value":1234
}
</code>
</pre>
- `get` - Get with a single key
<pre>
<code>
{
	"command":"get",
	"key":"AAA"
}
</code>
</pre>
- `getbulk` - Get the values for multiple keys from the cache
<pre>
<code>
{
	"command":"getBulk",
	"keys":["AAA","bbb","ccc"]
}
</code>
</pre> 
- `status` - Get the addresses of available and unavailable servers **! this command supported only as a synchronous blocking api !**
<pre>
<code>
{
	"command":"status"
}
</code>
</pre>
- `touch` - Touch the given key to reset its expiration time
<pre>
<code>
{
	"command":"touch",
	"key":"AAA"
	"exp":1000
}
</code>
</pre>
- `append` - Append to an existing value in the cache
<pre>
<code>
{
	"command":"append",
	"key":"AAA",
	"cas":0,
	"value":"5678"
}
</code>
</pre>
- `prepend` - Prepend to an existing value in the cache
<pre>
<code>
{
	"command":"prepend",
	"key":"AAA",
	"cas":0,
	"value":"-2-10 "
}
</code>
</pre>
- `add` - Add an object to the cache if it does not exist already
<pre>
<code>
{
	"command":"add",
	"key":"AAA",
	"cas":0,
	"value":"zzz"
}
</code>
</pre>
- `replace` - Replace an object with the given value if there is already a value for the given key
<pre>
<code>
{
	"command":"replace",
	"key":"AAA",
	"exp":0,
	"value":"yyyy"
}
</code>
</pre>
- `gat` - Get with a single key and reset its expiration
<pre>
<code>
{
	"command":"gat",
	"key":"AAA",
	"exp":0
}
</code>
</pre>
- `getstats` - Get all of the stats from all of the connections  **! this command supported only as a synchronous blocking api !** 
<pre>
<code>
{
	"command":"getStats"
}
</code>
</pre>
- `incr` - Increment the given key by the given amount
<pre>
<code>
{
	"command":"incr",
	"key":"AAA",
	"by":10
}
</code>
</pre>
- `decr` - Decrement the given key by the given value
<pre>
<code>
{
	"command":"decr",
	"key":"AAA",
	"by":10
}
</code>
</pre>
- `delete` - Delete the given key from the cache
<pre>
<code>
{
	"command":"delete",
	"key":"AAA"
}
</code>
</pre>
- `flush` - Flush all caches from all servers with a delay of application
<pre>
<code>
{
	"command":"flush"
}
</code>
</pre>

## Memcached responses ##

All system/infrastructure/network/etc errors will return with `"status":"error"`, e.g.:
<pre>
<code>
{
	"status":"error",
	"message":"error description"
}

</code>
</pre>

All successfully executed operations will return with  `"status":"ok"`.
Here is a json of such response:

<pre>
<code>
{
     "response":{ 
		"key":"aaa", 
		"value":1234
	 },
	 "status":"ok",
	 "command" : "get"
}
</code>
</pre>