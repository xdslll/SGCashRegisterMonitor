package com.sg.cash.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author xiads
 * @date 2018/5/24
 * @since
 */
public class DBUtil {

    public static void doExecute(Connection conn, BaseDbRunnable r) {
        // hive查询对象
        Statement stmt = null;
        try {
            // 获取查询对象
            stmt = conn.createStatement();
            r.run(stmt);
        } catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
