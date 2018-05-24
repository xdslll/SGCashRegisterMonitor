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
    private static String BASIC_PATH = "/config-basic.properties";
    private static String DEV_PATH ="/config-dev.properties";
    private static String PROD_PATH ="/config-prod.properties";

    static {
        try {
            Properties basicProp = new Properties();
            basicProp.load(ConfigUtil.class.getResourceAsStream(BASIC_PATH));
            String env = basicProp.getProperty("env");
            System.out.println("当前配置环境：" + env);

            p = new Properties();
            InputStream in;
            if (env.equals("dev")) {
                in = ConfigUtil.class.getResourceAsStream(DEV_PATH);
            } else {
                in = ConfigUtil.class.getResourceAsStream(PROD_PATH);
            }
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
