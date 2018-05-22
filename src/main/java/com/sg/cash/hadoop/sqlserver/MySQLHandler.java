package com.sg.cash.hadoop.sqlserver;

import com.sg.cash.hadoop.Client;
import org.apache.commons.lang.StringUtils;

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
    private static final String MYSQL_USER_NAME = Client.MYSQL_USER;

    /**
     * SQLServer密码
     */
    private static final String MYSQL_PWD = Client.MYSQL_PASSWORD;

    /**
     * SQLServer JDBC驱动名
     */
    private static final String MYSQL_DRIVER_NAME = Client.MYSQL_DRIVER_NAME;

    /**
     * SQLServer连接字符串
     */
    private static final String MYSQL_DB_URL = Client.MYSQL_URL;

    public Connection connectToMySQL() throws ClassNotFoundException, SQLException {
        Class.forName(MYSQL_DRIVER_NAME);
        Connection conn = DriverManager.getConnection(
                MYSQL_DB_URL, MYSQL_USER_NAME, MYSQL_PWD);
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
            System.out.println(getMachineNoSql);
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
            System.out.println(updateAvgMachineEffectiveSql);
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
            System.out.println(updateMachineRunningSql);
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
                .append(",'")
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
        System.out.println(sql);
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
            r++;
        } catch(Exception ex) {
            // ex.printStackTrace();
            System.out.println("插入失败，原因：" + ex.getMessage());
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

    public static void main(String[] args) {
        MySQLHandler handler = new MySQLHandler();
        Connection conn = null;
        try {
            conn = handler.connectToMySQL();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
