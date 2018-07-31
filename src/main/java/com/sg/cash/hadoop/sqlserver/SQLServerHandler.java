package com.sg.cash.hadoop.sqlserver;

import com.sg.cash.hadoop.Client;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xiads
 * @date 16/7/13
 */
public class SQLServerHandler {

    private  String sqlServerDriverName;
    private  String sqlServerDbUrl;
    private  String sqlServerUser;
    private  String sqlServerPwd;

    public SQLServerHandler(String sqlServerDriverName, String sqlServerDbUrl, String sqlServerUser, String sqlServerPwd) {
        this.sqlServerDriverName = sqlServerDriverName;
        this.sqlServerDbUrl = sqlServerDbUrl;
        this.sqlServerUser = sqlServerUser;
        this.sqlServerPwd = sqlServerPwd;
    }

    public Connection connect() throws ClassNotFoundException, SQLException {
        Class.forName(sqlServerDriverName);
        Connection conn = DriverManager.getConnection(
                sqlServerDbUrl, sqlServerUser, sqlServerPwd);
        System.out.println("连接SQLServer数据库成功");
        return conn;
    }

    /*public Connection connectToSQLServer() throws ClassNotFoundException, SQLException {
        Class.forName(SQL_SERVER_DRIVER_NAME);
        Connection conn = DriverManager.getConnection(
                SQL_SERVER_DB_URL, SQL_SERVER_USER_NAME, SQL_SERVER_PWD);
        System.out.println("连接SQL Server数据库成功");
        return conn;
    }*/

    public List<HupuData> getHupuData(Connection conn, String dbName) {
        String sql = "select f.AGT_GRP_ID as group_id, f.AGT_GRP_NAME as group_name, f.APP_AGT_ID as machine_id, f.AGT_NAME as machine_name," +
                "f.APP_DATE as date_str, f.ips as ip, f.openTime as open_time, g.activeTime as active_time " +
                "from (select a.APP_AGT_ID,a.APP_DATE,b.AGT_NAME,b.agt_grp_id, SUBSTRING( b.agt_ip_mac_str , CHARINDEX('(',b.agt_ip_mac_str)+1,CHARINDEX(')', " +
                "SUBSTRING( b.agt_ip_mac_str , CHARINDEX('(',b.agt_ip_mac_str)+1,16))-1) ips, c.agt_grp_name,sum(a.APP_ACTIVE)/3600 openTime " +
                "from [" + dbName + "].dbo.APP_REPORT a, " +
                "[ocular3].dbo.agent b," +
                "[ocular3].dbo.AGENT_group c " +
                "where a.APP_AGT_ID = b.AGT_ID " +
                "and a.APP_HASH = '#RUNNING#'  " +
                "and b.AGT_GRP_ID = c.AGT_GRP_ID " +
                "and b.agt_ip_mac_str is not null " +
                "and b.agt_ip_mac_str<>'' " +
                "group by a.APP_AGT_ID,a.APP_DATE,b.AGT_NAME,b.agt_grp_id,c.agt_grp_name, " +
                "SUBSTRING( b.agt_ip_mac_str , CHARINDEX('(',b.agt_ip_mac_str)+1,CHARINDEX(')',  " +
                "SUBSTRING( b.agt_ip_mac_str , CHARINDEX('(',b.agt_ip_mac_str)+1,16))-1)) f   " +
                "left join (select APP_AGT_ID,SUM(APP_ACTIVE)/3600 activeTime from [" + dbName + "].dbo.app_report where APP_NAME = 'SellSystem.exe' " +
                "group by APP_AGT_ID) g on f.APP_AGT_ID = g.APP_AGT_ID";
        System.out.println("数据库：" + dbName);
        System.out.println(sql);
        Statement stmt = null;
        ResultSet rs = null;
        ArrayList<HupuData> dataList = new ArrayList<>();
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
            while(rs.next()) {
                HupuData data = new HupuData();
                data.setGroupId(rs.getInt("group_id"));
                data.setGroupName(rs.getString("group_name"));
                data.setMachineId(rs.getInt("machine_id"));
                data.setMachineName(rs.getString("machine_name"));
                data.setDateStr(rs.getString("date_str"));
                data.setTimestamp(df.parse(data.getDateStr()).getTime() / 1000);
                data.setIp(rs.getString("ip"));
                data.setOpenTime(rs.getInt("open_time"));
                data.setActiveTime(rs.getInt("active_time"));
                dataList.add(data);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return dataList;
    }

    public static final Pattern PATTERN = Pattern.compile("^ocular3_data\\.[0-9]{8}$");

    public String[] getHupuDatabaseName(Connection conn) {
        String sql = "SELECT name FROM Master..SysDatabases ORDER BY name";
        Statement stmt = null;
        ResultSet rs = null;
        ArrayList<String> dbList = new ArrayList<>();
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            while(rs.next()) {
                String dbName = rs.getString("name");
                Matcher m = PATTERN.matcher(dbName.toLowerCase());
                if (m.find()) {
                    dbList.add(dbName);
                }
            }
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
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        // System.out.println(dbList);
        return dbList.toArray(new String[]{});
    }

    public void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int colCount = metaData.getColumnCount();
        int index = 1;
        while (rs.next()) {
            System.out.println("========== 第" + index++ + "行数据 ==========");
            for (int i = 1; i <= colCount; i++) {
                System.out.print("[" + metaData.getColumnName(i) + "]:");
                System.out.print(rs.getString(i) + "\n");
            }
        }
    }

    public static void main(String[] args) {
        syncHupuData();
    }

    public static void syncHupuData() {
        SQLServerHandler sqlServerHandler = new SQLServerHandler(
                Client.SQL_SERVER_DRIVER_NAME, Client.SQL_SERVER_DB_URL,
                Client.SQL_SERVER_USER, Client.SQL_SERVER_PWD);
        Connection sqlServerConn = null;
        Connection mySQLConn = null;
        try {
            // 连接互普SQLServer服务器
            sqlServerConn = sqlServerHandler.connect();
            // 获取互普所有数据库名
            String[] dbList = sqlServerHandler.getHupuDatabaseName(sqlServerConn);
            for (String db : dbList) {
                // 获取所有的互普数据
                List<HupuData> hupuDataList = sqlServerHandler.getHupuData(sqlServerConn, db);
                System.out.println(hupuDataList);
                // 将互普数据写入MySQL
                MySQLHandler mySQLHandler = new MySQLHandler(Client.MYSQL_DRIVER_NAME,
                        Client.MYSQL_URL, Client.MYSQL_USER, Client.MYSQL_PASSWORD);
                mySQLConn = mySQLHandler.connect();
                for (HupuData data : hupuDataList) {
                    if (data.getGroupName().equals("Unclassified")) {
                        continue;
                    }
                    // 将互普数据插入mysql
                    mySQLHandler.insertHupuData(mySQLConn, data);
                    mySQLHandler.updateHupuData(mySQLConn, data);
                }
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (sqlServerConn != null) {
                try {
                    sqlServerConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (mySQLConn != null) {
                try {
                    mySQLConn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void checkHupuDbNames(Connection conn) {
        String[] dbNames = getHupuDatabaseName(conn);
        for (String dbName : dbNames) {
            System.out.println(dbName);
        }
    }
}
