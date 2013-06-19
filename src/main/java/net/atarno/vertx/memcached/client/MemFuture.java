package net.atarno.vertx.memcached.client;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.concurrent.Future;

public class MemFuture {
    private long startTime;
    private Future future;
    private Message<JsonObject> message;
    private MemCommand command;

    public MemFuture( Future future, Message<JsonObject> message, MemCommand command ) {
        this.future = future;
        this.message = message;
        this.command = command;
        this.startTime = System.currentTimeMillis();
    }

    public Future getFuture() {
        return future;
    }

    public long getStartTime() {
        return startTime;
    }

    public Message<JsonObject> getMessage() {
        return message;
    }

    public MemCommand getCommand() {
        return command;
    }
}
