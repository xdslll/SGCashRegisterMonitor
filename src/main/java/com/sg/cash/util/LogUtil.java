package com.sg.cash.util;

import org.apache.commons.logging.Log;

/**
 * SAY SOMETHING ABOUT THE CLASS...
 *
 * @author xdslll
 * @date 2018/2/8
 **/
public class LogUtil {

    private static boolean canDebug = true;
    private static boolean canInfo = true;

    public static boolean canDebug() {
        return canDebug;
    }

    public static boolean canInfo() {
        return canInfo;
    }

    public static void debug(Log logger, Object o) {
        if (canDebug())
            logger.debug(o);
    }

    public static void info(Log logger, Object o) {
        if (canInfo())
            logger.info(o);
    }
}
