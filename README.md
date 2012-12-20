# Memcached busmod for Vert.x #

This module provides a Vert.x client for cache/storage servers, implementing memcached protocol. The name of this busmod is `vertx-memcached`. 

## Dependencies ##

Memcached connectivity is implemented using an asynchronous, Java written Spymemcached client.

More info about spymemcached project is available [here](http://code.google.com/p/spymemcached/ "spymemcached"). 
  

## Configuration ##

`vertx-memcached` is a worker mod. Each `vertx-memcached` worker incorporates one spymemcached client. Consider to use multiple `vertx-memcached` workers to achieve a connection-pool like functionality.

Here is an example of`vertx-memcached` worker configuration:

<pre>
<code>
{
    "address" : "vertx.memcached",
    "memcached_servers": "localhost:11211",
    "operation_timeout": 5000
}
</code>
</pre>
 
where

- `address` - the worker address on vert.x busmod. Mandatory.
- `memcached_servers` - the address of your memcached server(s). 1 to n space separated addresses can be passed, each of these should be in the following format: `<hostname:port>`. Mandatory.

- `operation_timeout` - to solve unexpected IO related issues between memcached client and server, all operations taking too long to execute are aborted after provided time-out (milliseconds). Optional, default - 10000 millis.

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