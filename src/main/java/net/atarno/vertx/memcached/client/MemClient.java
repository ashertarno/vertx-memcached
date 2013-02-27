package net.atarno.vertx.memcached.client;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.deploy.Verticle;

/**
 * spymemcached client for vert.x<p>
 * Please see the manual for a full description<p>
 *
 * @author <a href="mailto:atarno@gmail.com">Asher Tarnopolski</a>
 *         <p/>
 *         <p/>
 */
public class MemClient extends Verticle {
    private String address;
    private String memServers;
    private int timeOutMillis;
    private int taskCheckMillis;

    private EventBus eb;
    private Logger logger;
    private List<MemFuture> pending;
    private MemcachedClient[] memClients;
    private long timerTaskId = -1;

    @Override
    public void start() throws Exception {
        eb = vertx.eventBus();
        logger = container.getLogger();
        pending = new LinkedList<>();
        address = container.getConfig().getString("address", "vertx.memcached");
        memServers = container.getConfig().getString("memcached.servers");
        timeOutMillis = container.getConfig().getNumber("memcached.timeout.ms", 10000).intValue();
        taskCheckMillis = container.getConfig().getNumber("memcached.tasks.check.ms", 50).intValue();
        int connections = container.getConfig().getNumber("memcached.connections", 1).intValue();
        // init connection pool
        initMemClients(connections);

        // register verticle
        eb.registerHandler(address, memHandler, new AsyncResultHandler<Void>() {
            @Override
            public void handle(AsyncResult<Void> voidAsyncResult) {
                logger.info(this.getClass().getSimpleName() + " verticle is started");
            }
        });
    }

    Handler<Message<JsonObject>> memHandler = new Handler<Message<JsonObject>>() {
        public void handle(Message<JsonObject> message) {
            String command = MemCommand.voidNull(message.body.getString("command"));

            if (command.isEmpty()) {
                sendError(message, "\"command\" property is mandatory for request");
                return;
            }
            try {
                MemCommand mc = getByName(command);
                Future f = mc.query(getMemClient(), message);
                if (f.isDone()) {
                    if (shouldReply(message)) {
                        sendOK(message, mc.buildResponse(message, f, shouldReply(message)));
                    }
                } else {
                    MemFuture mf = new MemFuture(f, message, mc);
                    pending.add(mf);
                    if (timerTaskId == -1) {
                        timerTaskId = vertx.setPeriodic(taskCheckMillis, new Handler<Long>() {
                            public void handle(Long timerId) {
                                for (int i = pending.size() - 1; i >= 0; i--) {
                                    MemFuture mf = pending.get(i);
                                    boolean shouldReply = shouldReply(mf.getMessage());
                                    JsonObject r = null;
                                    if (mf.getFuture().isDone()) {
                                        pending.remove(i);
                                        try {
                                            r = mf.getCommand().buildResponse(mf.getMessage(), mf.getFuture(), shouldReply);
                                        } catch (TimeoutException e) {
                                            sendError(mf.getMessage(), "operation '" + mf.getCommand().name().toLowerCase() + "' timed out");
                                        } catch (ExecutionException e) {
                                            sendError(mf.getMessage(), "operation '" + mf.getCommand().name().toLowerCase() + "' failed");
                                        } catch (Exception e) {
                                            sendError(mf.getMessage(), e.getMessage());
                                        }
                                    } else if (System.currentTimeMillis() - mf.getStartTime() >= timeOutMillis) {
                                        mf.getFuture().cancel(true);
                                        pending.remove(i);
                                        if (shouldReply) {
                                            try {
                                                r = mf.getCommand().buildResponse(mf.getMessage(), null, shouldReply);
                                            } catch (TimeoutException e) {
                                                sendError(mf.getMessage(), "operation '" + mf.getCommand().name().toLowerCase() + "' timed out");
                                            } catch (ExecutionException e) {
                                                sendError(mf.getMessage(), "operation '" + mf.getCommand().name().toLowerCase() + "' failed");
                                            } catch (Exception e) {
                                                sendError(mf.getMessage(), e.getMessage());
                                            }
                                        }
                                    }
                                    if (r != null) {
                                        sendOK(mf.getMessage(), r);
                                    }
                                }
                                if (pending.isEmpty()) {
                                    vertx.cancelTimer(timerTaskId);
                                    timerTaskId = -1;
                                }
                            }
                        });
                    }
                }

            } catch (TimeoutException e) {
                sendError(message, "operation '" + command + "' timed out");
            } catch (IllegalArgumentException e) {
                sendError(message, "unknown command: '" + command + "'");
            } catch (ExecutionException e) {
                sendError(message, "operation '" + command + "' failed");
            } catch (Exception e) {
                sendError(message, e.getMessage());
            }
        }
    };

    private boolean shouldReply(Message<JsonObject> message) {
        return message.body.getBoolean("shouldReply") != null && message.body.getBoolean("shouldReply") == Boolean.TRUE;
    }

    private void sendOK(Message<JsonObject> message, JsonObject response) {
        JsonObject reply = new JsonObject();
        reply.putString("status", "ok");
        reply.putObject("response", response);

        message.reply(reply);
    }

    private void sendError(Message<JsonObject> message, String errMsg) {
        JsonObject reply = new JsonObject();
        reply.putString("status", "error");
        reply.putString("message", errMsg);

        message.reply(reply);
    }

    private void initMemClients(int connections) throws IOException {
        memClients = new MemcachedClient[connections < 1 ? 1 : connections];
        for (int i = 0; i < memClients.length; i++) {
            memClients[i] = new MemcachedClient(new BinaryConnectionFactory(), AddrUtil.getAddresses(memServers));
        }
    }

    private MemcachedClient getMemClient() {
        if (memClients.length == 1)
            return memClients[0];
        return memClients[(int) (Math.random() * memClients.length)];
    }

    private MemCommand getByName(String name) {
        if (name == null) {
            return null;
        }
        return MemCommand.valueOf(name.toUpperCase());
    }
}
