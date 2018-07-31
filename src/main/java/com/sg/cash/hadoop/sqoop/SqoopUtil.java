package com.sg.cash.hadoop.sqoop;

import com.jcraft.jsch.*;
import com.sg.cash.hadoop.Client;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.tool.SqoopTool;
import org.apache.sqoop.util.OptionsFileUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;

/**
 * @author xiads
 * @date 2018/4/19
 * @since
 */
public class SqoopUtil {

    public static boolean exportDataToMySQLByCmd(String sqoopFile) {
        int returnCode = 0;
        String cmd = "/opt/modules/sqoop-1.4.5-cdh5.3.6/bin/sqoop --options-file " + sqoopFile;
        JSch jsch = new JSch();
        MyUserInfo userInfo = new MyUserInfo();
        Vector<String> stdout = new Vector<>();
        Session session = null;
        Channel channel = null;
        BufferedReader br = null;
        try {
            session = jsch.getSession("hadoop", "www.mac-bigdata-01.com");
            session.setPassword("xds840126");
            session.setUserInfo(userInfo);
            session.connect();

            channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(cmd);

            channel.setInputStream(null);
            br = new BufferedReader(new InputStreamReader(channel
                    .getInputStream()));

            channel.connect();
            System.out.println("The remote command is: " + cmd);

            // Get the output of remote command.
            String line;
            while ((line = br.readLine()) != null) {
                stdout.add(line);
            }

            for (String str : stdout) {
                System.out.println(str);
            }

            // Get the return code only after the channel is closed.
            if (channel.isClosed()) {
                returnCode = channel.getExitStatus();
            }

            System.out.println("运行结果:" + (returnCode == 0 ? "成功" : "失败") + "(" + returnCode + ")");

            return true;
        } catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            // Disconnect the channel and session.
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    public static boolean clearTable(String sqoopTableName) {
        Connection conn = null;
        Statement stmt = null;
        String mysqlUrl = Client.MYSQL_URL;
        String mysqlUser = Client.MYSQL_USER;
        String mysqlPassword = Client.MYSQL_PASSWORD;
        try {
            // 清空现有MySQL数据
            Class.forName(Client.MYSQL_DRIVER_NAME);
            conn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
            System.out.println("连接mysql数据库成功");
            stmt = conn.createStatement();
            System.out.println("清空mysql表[" + sqoopTableName + "]");
            stmt.execute("truncate table " + sqoopTableName);
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class MyUserInfo implements UserInfo {

        @Override
        public String getPassphrase() {
            return null;
        }

        @Override
        public String getPassword() {
            System.out.println("MyUserInfo.getPassphrase()");
            return null;
        }

        @Override
        public boolean promptPassword(String message) {
            System.out.println("MyUserInfo.promptPassword()");
            System.out.println(message);
            return false;
        }

        @Override
        public boolean promptPassphrase(String message) {
            System.out.println("MyUserInfo.promptPassword()");
            System.out.println(message);
            return false;
        }

        @Override
        public boolean promptYesNo(String message) {
            System.out.println("MyUserInfo.promptYesNo()");
            System.out.println(message);
            if (message.contains("The authenticity of host")) {
                return true;
            }
            return false;
        }

        @Override
        public void showMessage(String message) {
            System.out.println("MyUserInfo.showMessage()");
        }
    }

    public static int exportDataToMySQL() {
        String mysqlUrl = Client.MYSQL_URL;
        String mysqlUser = Client.MYSQL_USER;
        String mysqlPassword = Client.MYSQL_PASSWORD;
        String sqoopMysqlUrl = Client.SQOOP_MYSQL_URL;
        String sqoopTableName = Client.SQOOP_TABLE_NAME;
        String sqoopHiveWarehouse = Client.SQOOP_HIVE_WAREHOUSE;
        String hdfsUri = Client.HDFS_REMOTE_URI;
        Connection conn = null;
        Statement stmt = null;
        try {
            // 清空现有MySQL数据
            Class.forName(Client.MYSQL_DRIVER_NAME);
            conn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword);
            System.out.println("连接mysql数据库成功");
            stmt = conn.createStatement();
            System.out.println("清空mysql表[" + sqoopTableName + "]");
            stmt.execute("truncate table " + sqoopTableName);

            String[] args = new String[]{
                    //"export",
                    "--connect", sqoopMysqlUrl,
                    "-username", mysqlUser,
                    "-password", mysqlPassword,
                    "--table", sqoopTableName,
                    "--export-dir", sqoopHiveWarehouse,
                    "-m", "1",
                    "--input-fields-terminated-by", "'001'",
                    "--input-null-string", "'\\\\N'",
                    "--input-null-non-string", "'\\\\N'"
            };
            String[] expandArgs = OptionsFileUtil.expandArguments(args);
            SqoopTool tool = SqoopTool.getTool("export");
            Configuration conf = new Configuration();
            conf.set("fs.default.name", hdfsUri);
            Configuration loadPlugins = SqoopTool.loadPlugins(conf);
            Sqoop sqoop = new Sqoop((com.cloudera.sqoop.tool.SqoopTool) tool, loadPlugins);
            return Sqoop.runSqoop(sqoop, expandArgs);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return -1;
    }

}

///Users/apple/.m2/repository/mysql/mysql-connector-java/5.1.45/mysql-connector-java-5.1.45.jar
