package net.atarno.vertx.memcached.client;

import net.spy.memcached.BinaryConnectionFactory;

/**
 * Date: 6/2/14
 * Time: 4:59 PM
 */
public class BinaryConnectionFactoryTO extends BinaryConnectionFactory {

    private long _operationTimeOut;

    public BinaryConnectionFactoryTO( long _operationTimeOut ) {

        super();
        this._operationTimeOut = _operationTimeOut;
    }

    @Override
    public long getOperationTimeout() {

        return _operationTimeOut;
    }
}
