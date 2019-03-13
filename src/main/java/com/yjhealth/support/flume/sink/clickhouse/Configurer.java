package com.yjhealth.support.flume.sink.clickhouse;

import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ck on 2019/3/11.
 * <p/>
 */
public class Configurer {

    private static final Logger logger = LoggerFactory.getLogger(Configurer.class);

    private String host;
    private String port;
    private String user;
    private String password;
    private String database;
    private String table;
    /**
     * Clickhouse格式化方式，Values/TabSeparate/JSONEachRow
     * 目前默认Values
     */
    private String format;
    /**
     * 单次事务从channel消费的数据数量
     */
    private int batchSize;
    /**
     * 期望写入Clickhouse的数据字段，eg:user_id,user_name
     */
    private List<String> expFields;
    /**
     * 采集的数据使用的分隔符，CSV标准默认为','
     */
    private String inDataDelimiter;

    public Configurer() {
    }

    /**
     * 初始化
     */
    public void init(Context context) throws IllegalAccessException {
        logger.info("clickhouse sink: configuration init.");
        Field[] fields = Configurer.class.getDeclaredFields();
        for (Field field : fields) {
            if (Modifier.isPrivate(field.getModifiers())
                    && !Modifier.isFinal(field.getModifiers())) {
                String val = context.getString(field.getName());
                Preconditions.checkArgument(val != null,
                        field.getName() +" must not be null!");
                field.setAccessible(true);
                if (String.class.isAssignableFrom(field.getType())) {
                    field.set(this, val);
                } else if (List.class.isAssignableFrom(field.getType())) {
                    field.set(this, Arrays.asList(val.split(",")));
                } else if (int.class.isAssignableFrom(field.getType())) {
                    field.set(this, Integer.parseInt(val));
                }
            }
        }
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public List<String> getExpFields() {
        return expFields;
    }

    public void setExpFields(List<String> expFields) {
        this.expFields = expFields;
    }

    public String getInDataDelimiter() {
        return inDataDelimiter;
    }

    public void setInDataDelimiter(String inDataDelimiter) {
        this.inDataDelimiter = inDataDelimiter;
    }
}
