package com.yjhealth.support.flume.sink.clickhouse;

import com.opencsv.CSVReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

/**
 * Created by ck on 2019/3/13.
 * <p/>
 */
public class InDataParser {

    private static final String CHARSET = "UTF-8";

    public static String[] parse(byte[] dataBody, String delimiter) {
        delimiter = delimiter.trim();
        try {
            CSVReader reader = new CSVReader(new InputStreamReader(new ByteArrayInputStream(dataBody), CHARSET));
            return reader.readAll().get(0);
        } catch (IOException e) {
            String dataStr = null;
            try {
                dataStr = new String(dataBody, CHARSET);
            } catch (UnsupportedEncodingException ignored) {}
            throw new UnsupportedOperationException(
                    String.format("Cannot parse data [%s], delimiter:[%s]", dataStr, delimiter));
        }
    }
}
