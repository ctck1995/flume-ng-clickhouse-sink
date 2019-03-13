package com.yjhealth.support.flume.sink.clickhouse;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnectionImpl;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.PreparedStatement;
import java.util.Map;

/**
 * Created by ck on 2019/3/11.
 * <p/>
 */
public class ClickhouseSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseSink.class);

    private Configurer configurer = null;
    private SinkCounter sinkCounter = null;

    @Override
    public void configure(Context context) {
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }
        if (configurer == null) {
            configurer = new Configurer();
        }
        try {
            configurer.init(context);
        } catch (IllegalAccessException ignored) {}
        Preconditions.checkArgument(configurer != null, "init configurer failed!");
    }

    @Override
    public synchronized void start() {
        logger.info("sink {} starting", getName());
        sinkCounter.start();
        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.info("sink {} stopping", getName());
        sinkCounter.incrementConnectionClosedCount();
        sinkCounter.stop();
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status;
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            String jdbcUrl = String.format("jdbc:clickhouse://%s:%s/%s", configurer.getHost(), configurer.getPort(),
                    configurer.getDatabase());
            ClickHouseProperties properties = new ClickHouseProperties().withCredentials(configurer.getUser(),
                    configurer.getPassword());
            properties.setUseServerTimeZone(false);
            BalancedClickhouseDataSource dataSource = new BalancedClickhouseDataSource(jdbcUrl, properties);
            ClickHouseConnectionImpl conn = (ClickHouseConnectionImpl) dataSource.getConnection();
            Map<String, String> schema = TableSchema.getSchema(conn, configurer.getTable());

            this.validateExpFields(schema);

            String sql = SQLHelper.initSql(configurer.getExpFields(), configurer.getDatabase(), configurer.getTable());
            PreparedStatement statement = conn.createPreparedStatement(sql);
            int count;
            for (count = 0; count < configurer.getBatchSize(); count++) {
                Event event = ch.take();
                if (event == null) {
                    break;
                }
                FieldRender render = new FieldRender(event.getBody(),
                        configurer.getExpFields(), configurer.getInDataDelimiter());
                if (render.isAc()) {
                    SQLHelper.makeUpSql(statement, schema, render, configurer.getExpFields()).addBatch();
                }
            }
            if (count <= 0) {
                sinkCounter.incrementBatchEmptyCount();
                txn.commit();
                return Status.BACKOFF;
            } else if (count < configurer.getBatchSize()) {
                sinkCounter.incrementBatchUnderflowCount();
            } else {
                sinkCounter.incrementBatchCompleteCount();
            }
            sinkCounter.addToEventDrainAttemptCount(count);
            statement.executeBatch();
            sinkCounter.incrementEventDrainSuccessCount();
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            status = Status.BACKOFF;
            logger.error(ExceptionUtils.getStackTrace(t), t);
            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {
            txn.close();
        }
        return status;
    }

    private void validateExpFields(Map<String, String> schema) {
        for (String field : configurer.getExpFields()) {
            if (!schema.containsKey(field)) {
                String msg = String.format("table [%s] doesn't contain field '%s'", configurer.getTable(), field);
                throw new UnsupportedOperationException(msg);
            }
        }
    }
}
