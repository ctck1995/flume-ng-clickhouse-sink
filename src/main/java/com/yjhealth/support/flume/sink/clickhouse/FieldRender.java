package com.yjhealth.support.flume.sink.clickhouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ck on 2019/3/12.
 * <p/>
 */
public class FieldRender {

    private static final Logger logger = LoggerFactory.getLogger(Configurer.class);

    private Map<String, String> data = new HashMap<>();
    private List<String> fields;
    private boolean ac = false;

    public FieldRender(byte[] dataBody, List<String> fields, String delimiter) {
        this.fields = fields;
        this.handleData(dataBody, delimiter);
    }

    public String getValue(String field) {
        return data.get(field);
    }

    public boolean isAc() {
        return this.ac;
    }

    private void handleData(byte[] dataBody, String delimiter) {
        try {
            String[] arr = InDataParser.parse(dataBody, delimiter);
            if (arr.length != fields.size()) {
                throw new IllegalArgumentException("data doesn't match the target field!");
            }
            String[] fieldNames = fields.toArray(new String[fields.size()]);
            for (int i = 0; i < arr.length; i++) {
                if ("".equals(arr[i])) {
                    data.put(fieldNames[i], null);
                } else {
                    data.put(fieldNames[i], arr[i]);
                }
            }
            ac = true;
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
