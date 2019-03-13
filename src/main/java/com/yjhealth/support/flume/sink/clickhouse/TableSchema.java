package com.yjhealth.support.flume.sink.clickhouse;

import ru.yandex.clickhouse.ClickHouseConnectionImpl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ck on 2019/3/11.
 * <p/>
 */
public class TableSchema {

    public static Map<String, String> getSchema(ClickHouseConnectionImpl connection, String table) throws SQLException {
        String sql = String.format("desc %s", table);
        ResultSet resultSet = connection.createStatement().executeQuery(sql);
        Map<String, String> schema = new HashMap<>();
        while(resultSet.next()) {
            schema.put(resultSet.getString(1), resultSet.getString(2));
        }
        return schema;
    }
}
