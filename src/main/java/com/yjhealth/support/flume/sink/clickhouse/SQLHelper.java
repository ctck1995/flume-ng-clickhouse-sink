package com.yjhealth.support.flume.sink.clickhouse;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by ck on 2019/3/12.
 * <p/>
 */
public class SQLHelper {

    private static final Logger logger = LoggerFactory.getLogger(SQLHelper.class);

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String initSql(List<String> fields, String database, String table) {
        return String.format("insert into %s.%s (%s) values (%s)", database,
                table,
                Joiner.on(", ").skipNulls().join(fields),
                paramPlaceHolder(fields.size()));
    }

    public static String paramPlaceHolder(int size) {
        List<String> list = new ArrayList<>();
        for (int i=0; i<size; i++) {
            list.add("?");
        }
        return Joiner.on(", ").skipNulls().join(list);
    }

    public static PreparedStatement makeUpSql(PreparedStatement statement, Map<String, String> schema,
            FieldRender render, List<String> fields) throws Exception {
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            String fieldType = schema.get(field);
            String fieldValue = render.getValue(field);
            switch (fieldType) {
                case "Int8":
                case "Int16":
                case "Int32":
                case "UInt8":
                case "UInt16":
                    if (fieldValue != null) {
                        try {
                            statement.setInt(i + 1, Integer.parseInt(fieldValue));
                        } catch (Exception e) {
                            logger.warn(String.format("Cannot Convert %s %s to integer, render default. Field is %s",
                                    fieldValue.getClass(), fieldValue, field));
                            statement.setInt(i + 1, 0);
                        }
                    } else {
                        statement.setInt(i + 1, 0);
                    }
                    break;
                case "UInt64":
                case "Int64":
                case "UInt32":
                    if (fieldValue != null) {
                        try {
                            statement.setLong(i + 1, Long.parseLong(fieldValue));
                        } catch (Exception e) {
                            logger.warn(String.format("Cannot Convert %s %s to long, render default",
                                    fieldValue.getClass(), fieldValue));
                            statement.setInt(i + 1, 0);
                        }
                    } else {
                        statement.setInt(i + 1, 0);
                    }
                    break;
                case "String":
                    if (fieldValue != null) {
                        statement.setString(i + 1, fieldValue);
                    } else {
                        statement.setString(i + 1, "");
                    }
                    break;
                case "DateTime":
                    if (fieldValue != null) {
                        try {
                            datetimeFormat.parse(fieldValue);
                            statement.setString(i + 1, fieldValue);
                        } catch (Exception e) {
                            logger.warn(String.format("Cannot Convert %s %s to datetime, render default. Field is %s",
                                    fieldValue.getClass(), fieldValue, field));
                            statement.setString(i + 1, "0000-00-00 00:00:00");
                        }
                    } else {
                        statement.setString(i + 1, "0000-00-00 00:00:00");
                    }
                    break;
                case "Date":
                    if (fieldValue != null) {
                        try {
                            dateFormat.parse(fieldValue);
                            statement.setString(i + 1, fieldValue);
                        } catch (Exception e) {
                            logger.warn(String.format("Cannot Convert %s %s to date, render default. Field is %s",
                                    fieldValue.getClass(), fieldValue, field));
                            statement.setString(i + 1, "0000-00-00");
                        }
                    } else {
                        statement.setString(i + 1, "0000-00-00");
                    }
                    break;
                case "Float32":
                case "Float64":
                    if (fieldValue != null) {
                        try {
                            statement.setFloat(i + 1, Float.parseFloat(fieldValue));
                        } catch (Exception e) {
                            logger.warn(String.format("Cannot Convert %s %s to float, render default",
                                    fieldValue.getClass(), fieldValue));
                            statement.setFloat(i + 1, 0f);
                        }
                    } else {
                        statement.setFloat(i + 1, 0f);
                    }
                    break;
            }
        }
        return statement;
    }
}
