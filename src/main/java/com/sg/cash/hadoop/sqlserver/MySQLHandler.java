package com.sg.cash.hadoop.sqlserver;

import com.sg.cash.model.MachineUsage;
import com.sg.cash.util.BaseDbRunnable;
import com.sg.cash.util.DBUtil;

import java.sql.*;

/**
 * @author xiads
 * @date 2018/5/18
 * @since
 */
public class MySQLHandler {

    /**
     * SQLServer用户名
     */
    private String mysqlUser;

    /**
     * SQLServer密码
     */
    private String mysqlPassword;

    /**
     * SQLServer JDBC驱动名
     */
    private String mysqlDriverName;

    /**
     * SQLServer连接字符串
     */
    private String mysqlUrl;

    public MySQLHandler(String mysqlDriverName, String mysqlUrl, String mysqlUser, String mysqlPassword) {
        this.mysqlDriverName = mysqlDriverName;
        this.mysqlUrl = mysqlUrl;
        this.mysqlUser = mysqlUser;
        this.mysqlPassword = mysqlPassword;
    }

    public Connection connect() throws ClassNotFoundException, SQLException {
        Class.forName(mysqlDriverName);
        Connection conn = DriverManager.getConnection(
                mysqlUrl, mysqlUser, mysqlPassword);
        System.out.println("连接MySQL数据库成功");
        return conn;
    }

    public int updateHupuData(Connection conn, HupuData data) {
        int r = 0;
        StringBuilder builder = new StringBuilder();
        String getMachineNoSql = builder.append("select machine_no from result_avg_machine_effective where ip='")
                .append(data.getIp())
                .append("' and store_no='")
                .append(data.getGroupName())
                .append("'")
                .toString();
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            // System.out.println(getMachineNoSql);
            ResultSet rs = stmt.executeQuery(getMachineNoSql);
            if (rs.next()) {
                String machineNo = rs.getString("machine_no");
                data.setMachineNo(machineNo);
            }
            builder = new StringBuilder();
            String updateAvgMachineEffectiveSql = builder.append("update result_avg_machine_effective set launch_time=")
                    .append(data.getOpenTime())
                    .append(", program_time=")
                    .append(data.getActiveTime())
                    .append(" where store_no='")
                    .append(data.getGroupName())
                    .append("' and ip='")
                    .append(data.getIp())
                    .append("' and create_dt=")
                    .append(data.getTimestamp())
                    .toString();
            // System.out.println(updateAvgMachineEffectiveSql);
            stmt.execute(updateAvgMachineEffectiveSql);
            builder = new StringBuilder();
            String updateMachineRunningSql = builder.append("update result_machine_running set machine_no='")
                    .append(data.getMachineNo() == null ? "" : data.getMachineNo())
                    .append("' where group_name='")
                    .append(data.getGroupName())
                    .append("' and ip='")
                    .append(data.getIp())
                    .append("' and date_str='")
                    .append(data.getDateStr())
                    .append("'")
                    .toString();
            // System.out.println(updateMachineRunningSql);
            stmt.execute(updateMachineRunningSql);
            r++;
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
        return r;
    }

    public int insertHupuData(Connection conn, HupuData data) {
        int r = 0;
        StringBuilder builder = new StringBuilder();
        String sql = builder.append("insert into result_machine_running values(")
                .append(data.getGroupId())
                .append(",'")
                .append(data.getGroupName())
                .append("',")
                .append(data.getMachineId())
                .append(",'','")
                .append(data.getMachineName())
                .append("','")
                .append(data.getDateStr())
                .append("',")
                .append(data.getTimestamp())
                .append(",'")
                .append(data.getIp())
                .append("',")
                .append(data.getOpenTime())
                .append(",")
                .append(data.getActiveTime())
                .append(")")
                .toString();
        // System.out.println(sql);
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
            r++;
        } catch(Exception ex) {
            // ex.printStackTrace();
            // System.out.println("插入失败，原因：" + ex.getMessage());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return r;
    }

    public int insertUsageData(Connection conn, MachineUsage data) {
        int r = 0;
        StringBuilder builder = new StringBuilder();
        String sql = builder.append("insert into sg_machine_usage values('")
                .append(data.getCity())
                .append("','")
                .append(data.getSmallArea())
                .append("','")
                .append(data.getStoreNo())
                .append("','")
                .append(data.getStoreName())
                .append("','")
                .append(data.getMachineNum())
                .append("','")
                .append(data.getUsageNum())
                .append("','")
                .append(data.getUsagePercent())
                .append("')")
                .toString();
        System.out.println(sql);
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
            r++;
        } catch(Exception ex) {
            ex.printStackTrace();
            // System.out.println("插入失败，原因：" + ex.getMessage());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return r;
    }

    public void checkDb(Connection conn, String dbName) {
        DBUtil.doExecute(conn, new BaseDbRunnable() {
            @Override
            public void run(Statement statement) throws Exception {
                String sql = "select table_name from information_schema.tables where table_schema='" + dbName + "'";
                ResultSet rs = statement.executeQuery(sql);
                while (rs.next()) {
                    System.out.println(rs.getString("table_name"));
                }
                rs.close();
            }
        });
    }
}
