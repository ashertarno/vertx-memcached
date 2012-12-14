/*
* Copyright 2012-2013 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package net.atarno.vertx.memcached;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient; 
import org.vertx.java.busmods.BusModBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Memcached client busmod<p>
 * Please see the manual for a full description<p>
 *
 * @author <a href="mailto:atarno@gmail.com">Asher Tarnopolski</a>
 *
 * <p>
 *
 */
public class MemClient extends BusModBase implements Handler<Message<JsonObject>>
{
   private String address;
   private String memServers;
   private long operationTimeOut;
   private TimeUnit timeUnit;

   private MemcachedClient memClient;

   @Override
   public void start()
   {
      super.start();
      address = getOptionalStringConfig("address", "vertx.memcached");
      memServers = getMandatoryStringConfig("memcached_servers");
      operationTimeOut = getOptionalLongConfig("operation_timeout", 5000);

      timeUnit = TimeUnit.MILLISECONDS;

      try
      {
         memClient = new MemcachedClient(new BinaryConnectionFactory(), AddrUtil.getAddresses(memServers));
      }
      catch (IOException e)
      {
         logger.error(e);
         logger.error("Failed to create memcached client with \"memcached_servers\"=\"" + memServers +"\". MemClient was not registered.");
         return;
      }
      eb.registerHandler(address, this);
      logger.debug("MemClient worker was registered as " + address);
   }

   @Override
   public void stop()
   {
      logger.debug("MemClient worker " + address +" was unregistered");
   }

   @Override
   public void handle(Message<JsonObject> message)
   {
      String command = voidNull(message.body.getString("command"));

      if(command.isEmpty())
      {
         sendError(message, "\"command\" property is mandatory for request");
         return;
      }
      //https://github.com/dustin/java-memcached-client/blob/master/src/main/java/net/spy/memcached/MemcachedClient.java
      try{
         switch (command)
         {
            case "set":
               set(message);
               break;
            case "get":
               get(message);
               break;
   /*         case "getBulk":
               getBulk(message);
               break;
            case "status":
               status(message);//server status,combine availableservers and unavailableservers
               break;
            case "touch":
               touch(message);
               break;
            case "append":
               append(message);
               break;
            case "prepend":
               prepend(message);
               break;
            case "add":
               add(message);
               break;
            case "replace":
               replace(message);
               break;
            case "getAndTouch":
               getAndTouch(message);
               break;
            case "getStats":
               getStats(message);
               break;
            case "incr":
               incr(message);
               break;
            case "decr":
               decr(message);
               break;
            case "delete":
               delete(message);
               break;
            case "flush":
               flush(message);
               break;
   */       default:
               sendError(message, "Unknown command: '" + command + "'");
         }
      }
      catch (InterruptedException e) {
         sendError(message, "Failed to complete the operation within " + operationTimeOut + " " + timeUnit + ".");
      }
      catch (ExecutionException e) {
         sendError(message, "Operation failed.");
      }
      catch (TimeoutException e) {
         sendError(message, "Failed to complete the operation within " + operationTimeOut + " " + timeUnit + ".");
      }
      catch (Exception e) {
         sendError(message, e.getMessage());
      }
   }

   private void set(Message<JsonObject> message) throws Exception
   {
      String key = voidNull(getMandatoryString("key", message));
      if(key.isEmpty())
      {
         return;
      }
      Object value = message.body.getField("value");
      int ttl = message.body.getInteger("ttl") == null ? 0 : message.body.getInteger("ttl");

      boolean success = memClient.set(key, ttl, value).get(operationTimeOut, timeUnit);

      if(!success)
      {
         sendError(message, "Operation failed.");
         return;
      }
      JsonObject response = new JsonObject();
      sendOK(message, response);
   }

   private void get(Message<JsonObject> message) throws Exception
   {
      String key = voidNull(getMandatoryString("key", message));
      if(key.isEmpty())
      {
         return;
      }
       
      Object value = memClient.asyncGet(key).get(operationTimeOut, timeUnit);
      JsonObject response = new JsonObject();
      try
      {
         response = put(response, value);
      } 
      catch (Exception e)
      {
         sendError(message, e.getMessage());
         return;   
      }
      sendOK(message, response);
   }

   private JsonObject put(JsonObject jsonObject, Object value) throws Exception
   {
      if(value != null)
      {
         //not serializable in current version of vert.x
   /*
         if(value instanceof JsonArray)
            jsonObject.putArray("value", (JsonArray) value);
         else if(value instanceof JsonObject)
            jsonObject.putObject("value", (JsonObject) value);
         else
   */

         if(value instanceof byte[])
            jsonObject.putBinary("value", (byte[]) value);
         else if(value instanceof Boolean)
            jsonObject.putBoolean("value", (Boolean) value);
         else if(value instanceof Number)
            jsonObject.putNumber("value", (Number) value);
         else if(value instanceof String)
            jsonObject.putString("value", (String) value);
         else
            throw new Exception("Unsupported object type");
      }
      return jsonObject;
   }
   
   private String voidNull(String s)
   {
      return s == null ? "" : s;
   }
}
