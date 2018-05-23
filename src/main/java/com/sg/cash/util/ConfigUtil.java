package com.sg.cash.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author xiads
 * @date 2018/5/23
 * @since
 */
public class ConfigUtil {

    private static Properties p;

    static {
        try {
            p = new Properties();
            InputStream in = ConfigUtil.class.getResourceAsStream("/config.properties");
            p.load(in);
        } catch(IOException ex) {
            ex.printStackTrace();
        }
    }
    public static String get(String key) {
        return p.getProperty(key);
    }

    public static Integer getInt(String key) {
        return Integer.valueOf(p.getProperty(key));
    }

}
