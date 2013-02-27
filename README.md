# Memcached mod for Vert.x #

This module provides a Vert.x client for cache/storage servers, implementing memcached protocol. The name of this mod is `vertx-memcached`. 

## Dependencies ##

Memcached connectivity is implemented using a non-blocking spymemcached client.

More info about spymemcached project is available [here](http://code.google.com/p/spymemcached/ "spymemcached"). 
  

## Configuration ##

`vertx-memcached` is a **non**-worker mod. Each `vertx-memcached` verticle can incorporate one-to-n spymemcached clients in a "pool". 

Here is an example of`vertx-memcached` worker configuration:

<pre>
<code>
{
    "address" : "vertx.memcached",
    "memcached.servers": "localhost:11211",
    "memcached.timeout.ms": 5000,
	"memcached.tasks.check.ms": 100,
    "memcached.connections": 2
}
</code>
</pre>
 
where

- `address` - the eventbus address of mod's verticles . Mandatory.
- `memcached.servers` - the address of your memcached server(s). 1 to n space separated addresses can be passed, each of these should be in the following format: `<hostname:port>`. Mandatory.

- `memcached.connections` - the number of spymemcached clients that will be initialized on verticle start up. These clients will be used randomly, in a way that mimics a connection pool behavior. Since spymemcached client is async and non-blocking, there is no need to use a lot of clients in such pool. Optional, default - 2.

- `memcached.tasks.check.ms` - to avoid verticle from being blocked while waiting for an operation that should return a result and is taking too long to complete, such operation is put to a queue, where it is repeatedly checked for completion. The completion of such operations will be checked every provided number of milliseconds. Optional, default - 50 ms.  

- `memcached.timeout.ms` - in case operations submitted to memcached server (see above) were not completed within number of milliseconds provided with this parameter, the operation is cancelled and time-out error is returned. Optional, default - 10000 ms.

 

## Usage ##

Memcached storage is done using key-value pairs, where key is always a String.
Since `JsonObject` and `JsonArray` are not serializable in current version of vert.x, only objects of the following types can be used as values to store:


- byte[]

- Boolean

- Number

- String  


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
- `status` - Get the addresses of available and unavailable servers
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
- `getstats` - Get all of the stats from all of the connections
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

All system/infrastructure/network/etc errors will return with status:error, f.e.:
<pre>
<code>
{
	"status":"error",
	"message":"failed to complete 'set' operation within 10000 milliseconds"
}

</code>
</pre>

All successfully executed operations will return with  `"status":"ok"`.
Here is a json of such response:

<pre>
<code>
{
     "response":{
				"data":{
						"key":1234
				},
				"success":true
	 },
	 "status":"ok"
}
</code>
</pre>

`response` block always includes a boolean `"success"` parameter, showing if the request operation was successful. This may be used mainly for logging and debugging. F.e., an attempt to get a value by key 'ZZZ' will return `"success":true` if the key is indeed found in cache. If this key is not found in cache, `"success":false,"reason":"failed to fetch key 'ZZZ'"` will return. 

** For all operations that do not return any value (f.e. set, delete, etc.), an option to receive an execution report is provided. To receive such report,
add `"shouldReply": true` to the message body. This is optional, by default no execution reports will be returned. Keep in mind that operations that should return a response (f.e. get, getBulk, etc.) will do so even if `"shouldReply": false` is passed. ** 