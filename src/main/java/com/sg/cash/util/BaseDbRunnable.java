package com.sg.cash.util;

import java.sql.Statement;

public abstract class BaseDbRunnable {

    /**
     * 执行sql语句
     *
     * @param statement
     * @throws Exception
     */
    public abstract void run(Statement statement) throws Exception;
}