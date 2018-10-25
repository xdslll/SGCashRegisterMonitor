package com.sg.cash.hadoop.hive;

import com.sg.cash.hadoop.Client;
import com.sg.cash.util.BaseDbRunnable;
import com.sg.cash.util.DBUtil;
import com.sg.cash.util.HdfsUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiads
 * @date 2018/3/30
 * @since
 */
public class HiveUtil {

    private static final String HIVE_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    private String hiveUrl;
    private String hiveUser;
    private String hivePassword;

    public HiveUtil(String hiveUrl, String hiveUser, String hivePassword) {
        this.hiveUrl = hiveUrl;
        this.hiveUser = hiveUser;
        this.hivePassword = hivePassword;
    }

    public Connection connect() throws ClassNotFoundException, SQLException {
        Class.forName(HIVE_DRIVER_NAME);
        return DriverManager.getConnection(hiveUrl, hiveUser, hivePassword);
    }

    public void print(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            for (int i = 0; i < columnCount; i++) {
                System.out.print(rs.getString(i + 1) + " ");
            }
            System.out.print("\n");
        }
    }

    public void query(Statement stmt, String sql) throws SQLException {
        ResultSet rs4 = stmt.executeQuery(sql);
        print(rs4);
        rs4.close();
    }

    public List<String> showDatabases(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("show databases");
        List<String> dbNameList = new ArrayList<>();
        while (rs.next()) {
            dbNameList.add(rs.getString(1));
        }
        return dbNameList;
    }

    public boolean existDatabase(Statement stmt, String dbName) throws SQLException {
        return showDatabases(stmt).contains(dbName);
    }

    public boolean createDatabase(Statement stmt, String dbName) throws SQLException {
        return stmt.execute(new StringBuilder()
                .append("create database ")
                .append(dbName)
                .toString());
    }

    public boolean createDatabase(Connection conn, String dbName) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            if (!existDatabase(stmt, dbName)) {
                System.out.println("数据库[" + dbName + "]不存在");
                return createDatabase(stmt, dbName);
            } else {
                System.out.println("数据库[" + dbName + "]已存在");
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            Client.close(stmt);
        }
        return false;
    }

    public boolean useDatabase(Statement stmt, String dbName) throws SQLException {
        return stmt.execute(new StringBuilder()
                .append("use ")
                .append(dbName)
                .toString());
    }

    public List<String> showTables(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("show tables");
        List<String> dbNameList = new ArrayList<>();
        while (rs.next()) {
            dbNameList.add(rs.getString(1));
        }
        return dbNameList;
    }

    public boolean existTable(Statement stmt, String tableName) throws SQLException {
        return showTables(stmt).contains(tableName);
    }

    public boolean createTable(Statement stmt, String sql) throws SQLException {
        return stmt.execute(sql);
    }

    public boolean importData(Statement stmt, String hdfsPath, String tableName, boolean overwrite) throws SQLException {
        StringBuilder sql = new StringBuilder()
                .append("LOAD DATA INPATH '")
                .append(hdfsPath)
                .append("' ");
        if (overwrite) {
            sql.append("overwrite ");
        }
        sql.append("INTO TABLE ")
                .append(tableName);
        System.out.print("开始导入数据:" + sql.toString());
        long _START = System.currentTimeMillis();
        boolean r = stmt.execute(sql.toString());
        long _END = System.currentTimeMillis();
        System.out.print("\t" + (double) (_END - _START) / 1000 + "s\n");
        return r;
    }

    public static boolean uploadStoreFile() {
        // hive连接对象
        Connection conn = null;
        // hive查询对象
        Statement stmt = null;
        // hive数据库名称
        String db = Client.HIVE_DB;
        // hive日志表
        String tblLog = Client.HIVE_TABLE_LOG;
        // hive门店表
        String tblStore = Client.HIVE_TABLE_STORE;
        // hive收银机表
        String tblMachine = Client.HIVE_TABLE_MACHINE;
        // hdfs对象
        FileSystem hdfs = null;
        // hdfs门店根目录
        String hdfsRootStorePath = Client.HDFS_UPLOAD_STORE_DIR;
        // hdfs收银机根目录
        String hdfsRootMachinePath = Client.HDFS_UPLOAD_MACHINE_DIR;
        // hdfs远程uri
        String hdfsRemoteUri = Client.HDFS_REMOTE_URI;
        String hdfsRemoteUri2 = Client.HDFS_REMOTE_URI2;
        // hdfs内部uri
        String hdfsInternalUri = Client.HDFS_INTERNAL_URI;
        // hdfs登录用户
        String user = Client.HDFS_USER;
        try {
            // 初始化hive客户端对象
            HiveUtil hiveClient = new HiveUtil(Client.HIVE_URL, Client.HIVE_USER, Client.HIVE_PASSWORD);
            // 连接hive客户端
            conn = hiveClient.connect();
            // 获取查询对象
            stmt = conn.createStatement();
            // 判断数据库是否存在
            System.out.println("是否包含[" + db + "]数据库:" + hiveClient.existDatabase(stmt, Client.HIVE_DB));
            if (!hiveClient.existDatabase(stmt, db)) {
                System.out.println("创建数据库[" + db + "]");
                hiveClient.createDatabase(stmt, db);
                System.out.println("是否包含[" + db + "]数据库:" + hiveClient.existDatabase(stmt, db));
            }
            System.out.println(hiveClient.showDatabases(stmt));
            // 使用数据库
            hiveClient.useDatabase(stmt, db);
            // 判断日志表是否存在，如果不存在则进行创建
            System.out.println("是否包含[" + tblLog + "]表:" + hiveClient.existTable(stmt, tblLog));
            if (!hiveClient.existTable(stmt, tblLog)) {
                System.out.println("创建表[" + tblLog + "]");
                String createLogTableSql = "CREATE TABLE " + db + ".tbl_sg_report_cash_detail (" +
                        "  create_dt bigint," +
                        "  id string," +
                        "  store_no string," +
                        "  machine_no string," +
                        "  emp_no string," +
                        "  emp_name string," +
                        "  print_page int," +
                        "  print_line int," +
                        "  print_begin_time string," +
                        "  print_end_time string," +
                        "  ip string," +
                        "  money int" +
                        ") row format delimited fields terminated by \"|\"";
                hiveClient.createTable(stmt, createLogTableSql);
                System.out.println("是否包含[" + tblLog + "]表:" + hiveClient.existTable(stmt, tblLog));
            }

            // 判断门店表是否存在，如果不存在则进行创建
            System.out.println("是否包含[" + tblStore + "]表:" + hiveClient.existTable(stmt, tblStore));
            if (!hiveClient.existTable(stmt, tblStore)) {
                System.out.println("创建表[" + tblStore + "]");
                String createStoreTableSql = "CREATE TABLE " + db + ".tbl_sg_store_info (" +
                        "  store_no string," +
                        "  store_name string," +
                        "  type string," +
                        "  area string," +
                        "  city string," +
                        "  small_area string" +
                        ") row format delimited fields terminated by \",\"";
                hiveClient.createTable(stmt, createStoreTableSql);
                System.out.println("是否包含[" + tblStore + "]表:" + hiveClient.existTable(stmt, tblStore));
            }

            // 判断收银机表是否存在，如果不存在则进行创建
            System.out.println("是否包含[" + tblMachine + "]表:" + hiveClient.existTable(stmt, tblMachine));
            if (!hiveClient.existTable(stmt, tblMachine)) {
                System.out.println("创建表[" + tblMachine + "]");
                String createMachineTableSql = "CREATE TABLE " + db + ".tbl_sg_machine_info (" +
                        "  store_no string," +
                        "  machine_num int" +
                        ") row format delimited fields terminated by \",\"";
                hiveClient.createTable(stmt, createMachineTableSql);
                System.out.println("是否包含[" + tblMachine + "]表:" + hiveClient.existTable(stmt, tblMachine));
            }

            System.out.println(hiveClient.showTables(stmt));

            String activeHdfsRemoteUri = com.sg.cash.hadoop.hdfs.HdfsUtil.checkActiveHdfs(hdfsRemoteUri, hdfsRemoteUri2, user);
            // 生成配置文件
            Configuration conf = new Configuration();
            // 生成hdfs对象
            hdfs = FileSystem.get(
                    new URI(activeHdfsRemoteUri),
                    conf,
                    user
            );
            // 生成门店文件根路径
            Path hdfsStorePath = new Path(hdfsRootStorePath);
            FileStatus[] storeFileList = hdfs.listStatus(hdfsStorePath);
            if (storeFileList != null && storeFileList.length > 0) {
                // 生成hive识别的hdfs路径
                String hiveStorePath = new StringBuilder()
                        .append(hdfsInternalUri)
                        .append(hdfsRootStorePath)
                        .toString();
                // 导入数据
                hiveClient.importData(stmt, hiveStorePath, db + "." + tblStore, true);
            } else {
                System.out.println("文件夹[" + hdfsStorePath + "]下文件为空,无需导入");
            }
            // 生成收银机文件根路径
            Path hdfsMachinePath = new Path(hdfsRootMachinePath);
            FileStatus[] machineFileList = hdfs.listStatus(hdfsMachinePath);
            if (machineFileList != null && machineFileList.length > 0) {
                // 生成hive识别的hdfs路径
                String hiveMachinePath = new StringBuilder()
                        .append(hdfsInternalUri)
                        .append(hdfsRootMachinePath)
                        .toString();
                // 导入数据
                hiveClient.importData(stmt, hiveMachinePath, db + "." + tblMachine, true);
            } else {
                System.out.println("文件夹[" + hdfsMachinePath + "]下文件为空,无需导入");
            }
            return true;
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
            if (hdfs != null) {
                try {
                    hdfs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    public static boolean uploadToHiveWarehouse() {
        // hive连接对象
        Connection conn = null;
        // hive查询对象
        Statement stmt = null;
        // hive数据库名称
        String db = Client.HIVE_DB;
        // hive日志表
        String tblLog = Client.HIVE_TABLE_LOG;
        // hive门店表
        String tblStore = Client.HIVE_TABLE_STORE;
        // hive收银机表
        String tblMachine = Client.HIVE_TABLE_MACHINE;
        // hdfs对象
        FileSystem hdfs = null;
        // hdfs日志根目录
        String hdfsRootLogPath = Client.HDFS_REPORT_INPUT_DIR;
        // hdfs门店根目录
        String hdfsRootStorePath = Client.HDFS_UPLOAD_STORE_DIR;
        // hdfs收银机根目录
        String hdfsRootMachinePath = Client.HDFS_UPLOAD_MACHINE_DIR;
        // hdfs远程uri
        String hdfsRemoteUri = Client.HDFS_REMOTE_URI;
        String hdfsRemoteUri2 = Client.HDFS_REMOTE_URI2;
        // hdfs内部uri
        String hdfsInternalUri = Client.HDFS_INTERNAL_URI;
        // hdfs登录用户
        String user = Client.HDFS_USER;
        try {
            // 初始化hive客户端对象
            HiveUtil hiveClient = new HiveUtil(Client.HIVE_URL, Client.HIVE_USER, Client.HIVE_PASSWORD);
            // 连接hive客户端
            conn = hiveClient.connect();
            // 获取查询对象
            stmt = conn.createStatement();
            // 判断数据库是否存在
            System.out.println("是否包含[" + db + "]数据库:" + hiveClient.existDatabase(stmt, Client.HIVE_DB));
            if (!hiveClient.existDatabase(stmt, db)) {
                System.out.println("创建数据库[" + db + "]");
                hiveClient.createDatabase(stmt, db);
                System.out.println("是否包含[" + db + "]数据库:" + hiveClient.existDatabase(stmt, db));
            }
            System.out.println(hiveClient.showDatabases(stmt));
            // 使用数据库
            hiveClient.useDatabase(stmt, db);
            // 判断日志表是否存在，如果不存在则进行创建
            System.out.println("是否包含[" + tblLog + "]表:" + hiveClient.existTable(stmt, tblLog));
            if (!hiveClient.existTable(stmt, tblLog)) {
                System.out.println("创建表[" + tblLog + "]");
                String createLogTableSql = "CREATE TABLE " + db + ".tbl_sg_report_cash_detail (" +
                        "  create_dt bigint," +
                        "  id string," +
                        "  store_no string," +
                        "  machine_no string," +
                        "  emp_no string," +
                        "  emp_name string," +
                        "  print_page int," +
                        "  print_line int," +
                        "  print_begin_time string," +
                        "  print_end_time string," +
                        "  ip string," +
                        "  money int" +
                        ") row format delimited fields terminated by \"|\"";
                hiveClient.createTable(stmt, createLogTableSql);
                System.out.println("是否包含[" + tblLog + "]表:" + hiveClient.existTable(stmt, tblLog));
            }

            // 判断门店表是否存在，如果不存在则进行创建
            System.out.println("是否包含[" + tblStore + "]表:" + hiveClient.existTable(stmt, tblStore));
            if (!hiveClient.existTable(stmt, tblStore)) {
                System.out.println("创建表[" + tblStore + "]");
                String createStoreTableSql = "CREATE TABLE " + db + ".tbl_sg_store_info (" +
                        "  store_no string," +
                        "  store_name string," +
                        "  type string," +
                        "  area string," +
                        "  city string," +
                        "  small_area string" +
                        ") row format delimited fields terminated by \",\"";
                hiveClient.createTable(stmt, createStoreTableSql);
                System.out.println("是否包含[" + tblStore + "]表:" + hiveClient.existTable(stmt, tblStore));
            }

            // 判断收银机表是否存在，如果不存在则进行创建
            System.out.println("是否包含[" + tblMachine + "]表:" + hiveClient.existTable(stmt, tblMachine));
            if (!hiveClient.existTable(stmt, tblMachine)) {
                System.out.println("创建表[" + tblMachine + "]");
                String createMachineTableSql = "CREATE TABLE " + db + ".tbl_sg_machine_info (" +
                        "  store_no string," +
                        "  machine_num int" +
                        ") row format delimited fields terminated by \",\"";
                hiveClient.createTable(stmt, createMachineTableSql);
                System.out.println("是否包含[" + tblMachine + "]表:" + hiveClient.existTable(stmt, tblMachine));
            }

            System.out.println(hiveClient.showTables(stmt));

            String activeHdfsRemoteUri = com.sg.cash.hadoop.hdfs.HdfsUtil.checkActiveHdfs(hdfsRemoteUri, hdfsRemoteUri2, user);
            // 生成配置文件
            Configuration conf = new Configuration();
            // 生成hdfs对象
            hdfs = FileSystem.get(
                    new URI(activeHdfsRemoteUri),
                    conf,
                    user
            );
            // 如果有文件更新才进行重新计算
            boolean needUpdate = false;
            // 生成日志文件根路径
            Path hdfsLogPath = new Path(hdfsRootLogPath);
            // 获取根路径下的所有文件
            FileStatus[] logFileList = hdfs.listStatus(hdfsLogPath);
            if (logFileList != null && logFileList.length > 0) {
                needUpdate = true;
                for (FileStatus file : logFileList) {
                    // 如果是文件夹则将文件夹下的所有文件导入hive
                    if (file.isDirectory()) {
                        FileStatus[] subFileList = hdfs.listStatus(file.getPath());
                        if (subFileList == null || subFileList.length == 0) {
                            continue;
                        }
                        // 生成hive识别的hdfs路径
                        String hiveLogPath = new StringBuilder()
                                .append(hdfsInternalUri)
                                .append(hdfsRootLogPath)
                                .append(file.getPath().getName())
                                .toString();
                        // 导入数据
                        hiveClient.importData(stmt, hiveLogPath, db + "." + tblLog, false);
                    }
                }
            } else {
                System.out.println("文件夹[" + hdfsLogPath + "]下文件为空,无需导入");
            }
            // 生成门店文件根路径
            Path hdfsStorePath = new Path(hdfsRootStorePath);
            FileStatus[] storeFileList = hdfs.listStatus(hdfsStorePath);
            if (storeFileList != null && storeFileList.length > 0) {
                // 生成hive识别的hdfs路径
                String hiveStorePath = new StringBuilder()
                        .append(hdfsInternalUri)
                        .append(hdfsRootStorePath)
                        .toString();
                // 导入数据
                hiveClient.importData(stmt, hiveStorePath, db + "." + tblStore, true);
            } else {
                System.out.println("文件夹[" + hdfsStorePath + "]下文件为空,无需导入");
            }
            // 生成收银机文件根路径
            Path hdfsMachinePath = new Path(hdfsRootMachinePath);
            FileStatus[] machineFileList = hdfs.listStatus(hdfsMachinePath);
            if (machineFileList != null && machineFileList.length > 0) {
                // 生成hive识别的hdfs路径
                String hiveMachinePath = new StringBuilder()
                        .append(hdfsInternalUri)
                        .append(hdfsRootMachinePath)
                        .toString();
                // 导入数据
                hiveClient.importData(stmt, hiveMachinePath, db + "." + tblMachine, true);
            } else {
                System.out.println("文件夹[" + hdfsMachinePath + "]下文件为空,无需导入");
            }
            if (needUpdate) {
                // 开始生成收银员报表
                String dropResultAvgTimeSql = "drop table " + db + ".result_avg_cash_time";
                String createResultAvgTimeSql = "create table " + db + ".result_avg_cash_time as " +
                        "select round((t1.total_time / t1.total_sku), 2) as avg_cash_time, round(t1.total_time, 2) as total_time, " +
                        "t1.total_sku, t1.total_page, t1.total_order, t1.emp_no, t1.emp_name, t1.store_no, " +
                        "unix_timestamp(create_dt, 'yyyyMMdd') as create_dt, t2.store_name, t2.type, t2.area, " +
                        "t2.city, t2.small_area, round((t1.total_order/(t1.total_time/3600)), 2) as avg_order_time, t1.money " +
                        "from (select sum(print_end_time - print_begin_time) as total_time, sum(print_line) as total_sku, " +
                        "sum(print_page) as total_page, count(*) as total_order, emp_no, emp_name, store_no, create_dt, " +
                        "sum(money) as money " +
                        "from (select substring(create_dt,1,8) as create_dt, store_no, emp_no, emp_name, print_line, print_page, " +
                        "unix_timestamp(substring(print_begin_time,0,14),'yyyyMMddHHmmss') as print_begin_time, " +
                        "unix_timestamp(substring(print_end_time,0,14),'yyyyMMddHHmmss') as print_end_time, money " +
                        "from tbl_sg_report_cash_detail " +
                        "where !isnull(print_end_time) and !isnull(print_begin_time) and !isnull(create_dt) and !isnull(money) and money>0) " +
                        "as t1 " +
                        "where (print_end_time - print_begin_time) <= 7200  and (print_end_time - print_begin_time) >= 0 " +
                        "group by emp_no, emp_name, store_no, create_dt) as t1 " +
                        "left join tbl_sg_store_info t2 on t1.store_no=t2.store_no";
                        //"create table " + db + ".result_avg_cash_time as select round((t1.total_time / t1.total_sku), 2) as avg_cash_time, round(t1.total_time, 2) as total_time, t1.total_sku, t1.total_page, t1.total_order, t1.emp_no, t1.emp_name, t1.store_no, unix_timestamp(create_dt, 'yyyyMMdd') as create_dt, t2.store_name, t2.type, t2.area, t2.city, t2.small_area, round((t1.total_order/(t1.total_time/3600)), 2) as avg_order_time, t1.money " +
                        //"from (select sum(print_end_time - print_begin_time) as total_time, sum(print_line) as total_sku, sum(print_page) as total_page, count(*) as total_order, emp_no, emp_name, store_no, create_dt, sum(money) as money from (select substring(create_dt,1,8) as create_dt, store_no, emp_no, emp_name, print_line, print_page, unix_timestamp(substring(print_begin_time,0,14),'yyyyMMddHHmmss') as print_begin_time, unix_timestamp(substring(print_end_time,0,14),'yyyyMMddHHmmss') as print_end_time, money from tbl_sg_report_cash_detail where !isnull(print_end_time) and !isnull(print_begin_time) and !isnull(create_dt)) as t1 " +
                        //"where (print_end_time - print_begin_time) <= 7200 group by emp_no, emp_name, store_no, create_dt) as t1 left join tbl_sg_store_info t2 on t1.store_no=t2.store_no";
                System.out.println("丢弃表[result_avg_cash_time]...");
                stmt.execute(dropResultAvgTimeSql);
                System.out.println("丢弃表[result_avg_cash_time]成功");
                System.out.println("生成表[result_avg_cash_time]...");
                //System.out.println(createResultAvgTimeSql);
                stmt.execute(createResultAvgTimeSql);
                System.out.println("生成表[result_avg_cash_time]成功");
                // 开始生成收银机报表
                String dropResultAvgMachineEffectSql = "drop table " + db + ".result_avg_machine_effective";
                String createResultAvgMachineEffectSql = "create table result_avg_machine_effective as select " +
                        "round(t1.total_time, 2) as total_time, t1.total_sku, t1.total_page, t1.total_order, t1.machine_no, " +
                        "t1.store_no, unix_timestamp(create_dt, 'yyyyMMdd') as create_dt, t2.store_name, t2.type, t2.area, t2.city, " +
                        "t2.small_area, round((t1.total_sku/(t1.total_time/3600)), 2) as avg_machine_effective, t3.machine_num, t1.ip, t1.launch_time, t1.program_time " +
                        "from (select sum(print_end_time - print_begin_time) as total_time, sum(print_line) as total_sku, " +
                        "sum(print_page) as total_page, count(*) as total_order, machine_no, store_no, create_dt, ip, " +
                        "sum(launch_time) as launch_time, sum(program_time) as program_time " +
                        "from (select substring(create_dt,1,8) as create_dt, store_no, machine_no, print_line, print_page, " +
                        "unix_timestamp(substring(print_begin_time,0,14),'yyyyMMddHHmmss') as print_begin_time, " +
                        "unix_timestamp(substring(print_end_time,0,14),'yyyyMMddHHmmss') as print_end_time, ip," +
                        "round(0) as launch_time, round(0) as program_time from " +
                        "tbl_sg_report_cash_detail where !isnull(print_end_time) and !isnull(print_begin_time) and !isnull(create_dt) and !isnull(money) and money>0) " +
                        "as t1 where (print_end_time - print_begin_time) <= 7200 and (print_end_time - print_begin_time) >= 0 group by machine_no, store_no, create_dt, ip) as t1 " +
                        "left join tbl_sg_store_info t2 on t1.store_no=t2.store_no " +
                        "left join tbl_sg_machine_info t3 on t1.store_no=t3.store_no";
                System.out.println("丢弃表[result_avg_machine_effective]...");
                stmt.execute(dropResultAvgMachineEffectSql);
                System.out.println("丢弃表[result_avg_machine_effective]成功");
                System.out.println("生成表[result_avg_machine_effective]...");
                stmt.execute(createResultAvgMachineEffectSql);
                System.out.println("生成表[result_avg_machine_effective]成功");
            }
            return true;
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
            if (hdfs != null) {
                try {
                    hdfs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    public static void main(String[] args) {
        uploadToHiveWarehouse();
    }

    public void checkDb(Connection conn, String hiveDb) {
        DBUtil.doExecute(conn, new BaseDbRunnable() {
            @Override
            public void run(Statement statement) throws SQLException {
                if (existDatabase(statement, hiveDb)) {
                    System.out.println("数据库[" + hiveDb + "]存在");
                } else {
                    System.out.println("数据库[" + hiveDb + "]不存在");
                }
            }
        });
    }

    public void checkTable(Connection conn, String hiveDb, String hiveTableLog) {
        DBUtil.doExecute(conn, new BaseDbRunnable() {
            @Override
            public void run(Statement statement) throws SQLException {
                if (existTable(statement, hiveDb + "." + hiveTableLog)) {
                    System.out.println("数据库[" + hiveDb + "." + hiveTableLog + "]存在");
                } else {
                    System.out.println("数据库[" + hiveDb + "." + hiveTableLog + "]不存在");
                }
            }
        });
    }

    public void genAvgMachineEffective(Connection conn, String db) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            // 开始生成收银机报表
            String dropResultAvgMachineEffectSql = "drop table " + db + ".result_avg_machine_effective";
            String createResultAvgMachineEffectSql = "create table result_avg_machine_effective as select " +
                    "round(t1.total_time, 2) as total_time, t1.total_sku, t1.total_page, t1.total_order, t1.machine_no, " +
                    "t1.store_no, unix_timestamp(create_dt, 'yyyyMMdd') as create_dt, t2.store_name, t2.type, t2.area, t2.city, " +
                    "t2.small_area, round((t1.total_sku/(t1.total_time/3600)), 2) as avg_machine_effective, t3.machine_num, t1.ip, t1.launch_time, t1.program_time " +
                    "from (select sum(print_end_time - print_begin_time) as total_time, sum(print_line) as total_sku, " +
                    "sum(print_page) as total_page, count(*) as total_order, machine_no, store_no, create_dt, ip, " +
                    "sum(launch_time) as launch_time, sum(program_time) as program_time " +
                    "from (select substring(create_dt,1,8) as create_dt, store_no, machine_no, print_line, print_page, " +
                    "unix_timestamp(substring(print_begin_time,0,14),'yyyyMMddHHmmss') as print_begin_time, " +
                    "unix_timestamp(substring(print_end_time,0,14),'yyyyMMddHHmmss') as print_end_time, ip," +
                    "round(0) as launch_time, round(0) as program_time from " +
                    "tbl_sg_report_cash_detail where !isnull(print_end_time) and !isnull(print_begin_time) and !isnull(create_dt) and !isnull(money) and money>0) " +
                    "as t1 where (print_end_time - print_begin_time) <= 7200 and (print_end_time - print_begin_time) >= 0 group by machine_no, store_no, create_dt, ip) as t1 " +
                    "left join tbl_sg_store_info t2 on t1.store_no=t2.store_no " +
                    "left join tbl_sg_machine_info t3 on t1.store_no=t3.store_no";
            System.out.println("丢弃表[result_avg_machine_effective]...");
            stmt.execute(dropResultAvgMachineEffectSql);
            System.out.println("丢弃表[result_avg_machine_effective]成功");
            System.out.println("生成表[result_avg_machine_effective]...");
            stmt.execute(createResultAvgMachineEffectSql);
            System.out.println("生成表[result_avg_machine_effective]成功");
        } catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            Client.close(stmt);
        }
    }

    public void genAvgCashTime(Connection conn, String db) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            String dropResultAvgTimeSql = "drop table " + db + ".result_avg_cash_time";
            String createResultAvgTimeSql = "create table " + db + ".result_avg_cash_time as " +
                    "select round((t1.total_time / t1.total_sku), 2) as avg_cash_time, round(t1.total_time, 2) as total_time, " +
                    "t1.total_sku, t1.total_page, t1.total_order, t1.emp_no, t1.emp_name, t1.store_no, " +
                    "unix_timestamp(create_dt, 'yyyyMMdd') as create_dt, t2.store_name, t2.type, t2.area, " +
                    "t2.city, t2.small_area, round((t1.total_order/(t1.total_time/3600)), 2) as avg_order_time, t1.money " +
                    "from (select sum(print_end_time - print_begin_time) as total_time, sum(print_line) as total_sku, " +
                    "sum(print_page) as total_page, count(*) as total_order, emp_no, emp_name, store_no, create_dt, " +
                    "sum(money) as money " +
                    "from (select substring(create_dt,1,8) as create_dt, store_no, emp_no, emp_name, print_line, print_page, " +
                    "unix_timestamp(substring(print_begin_time,0,14),'yyyyMMddHHmmss') as print_begin_time, " +
                    "unix_timestamp(substring(print_end_time,0,14),'yyyyMMddHHmmss') as print_end_time, money " +
                    "from tbl_sg_report_cash_detail " +
                    "where !isnull(print_end_time) and !isnull(print_begin_time) and !isnull(create_dt) and !isnull(money) and money>0) " +
                    "as t1 " +
                    "where (print_end_time - print_begin_time) <= 7200  and (print_end_time - print_begin_time) >= 0 " +
                    "group by emp_no, emp_name, store_no, create_dt) as t1 " +
                    "left join tbl_sg_store_info t2 on t1.store_no=t2.store_no";
            //"create table " + db + ".result_avg_cash_time as select round((t1.total_time / t1.total_sku), 2) as avg_cash_time, round(t1.total_time, 2) as total_time, t1.total_sku, t1.total_page, t1.total_order, t1.emp_no, t1.emp_name, t1.store_no, unix_timestamp(create_dt, 'yyyyMMdd') as create_dt, t2.store_name, t2.type, t2.area, t2.city, t2.small_area, round((t1.total_order/(t1.total_time/3600)), 2) as avg_order_time, t1.money " +
            //"from (select sum(print_end_time - print_begin_time) as total_time, sum(print_line) as total_sku, sum(print_page) as total_page, count(*) as total_order, emp_no, emp_name, store_no, create_dt, sum(money) as money from (select substring(create_dt,1,8) as create_dt, store_no, emp_no, emp_name, print_line, print_page, unix_timestamp(substring(print_begin_time,0,14),'yyyyMMddHHmmss') as print_begin_time, unix_timestamp(substring(print_end_time,0,14),'yyyyMMddHHmmss') as print_end_time, money from tbl_sg_report_cash_detail where !isnull(print_end_time) and !isnull(print_begin_time) and !isnull(create_dt)) as t1 " +
            //"where (print_end_time - print_begin_time) <= 7200 group by emp_no, emp_name, store_no, create_dt) as t1 left join tbl_sg_store_info t2 on t1.store_no=t2.store_no";
            System.out.println("丢弃表[result_avg_cash_time]...");
            stmt.execute(dropResultAvgTimeSql);
            System.out.println("丢弃表[result_avg_cash_time]成功");
            System.out.println("生成表[result_avg_cash_time]...");
            //System.out.println(createResultAvgTimeSql);
            stmt.execute(createResultAvgTimeSql);
            System.out.println("生成表[result_avg_cash_time]成功");
        } catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            Client.close(stmt);
        }
    }

    public void uploadFileToHive(Connection conn, String hdfsRemoteUri, String hdfsRemoteUri2,
                                 String user, String hdfsRootLogPath, String hdfsInternalUri, String tableName) {
        FileSystem hdfs = null;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            String activeHdfsRemoteUri = com.sg.cash.hadoop.hdfs.HdfsUtil.checkActiveHdfs(hdfsRemoteUri, hdfsRemoteUri2, user);
            System.out.println("当前激活的hdfs uri:" + activeHdfsRemoteUri);
            if (activeHdfsRemoteUri == null) {
                throw new IOException("当前无可用的hdfs uri!");
            }
            // 生成配置文件
            Configuration conf = new Configuration();
            // 生成hdfs对象
            hdfs = FileSystem.get(
                    new URI(activeHdfsRemoteUri),
                    conf,
                    user
            );
            // 如果有文件更新才进行重新计算
            boolean needUpdate = false;
            // 生成日志文件根路径
            Path hdfsLogPath = new Path(hdfsRootLogPath);
            // 获取根路径下的所有文件
            FileStatus[] logFileList = hdfs.listStatus(hdfsLogPath);
            if (logFileList != null && logFileList.length > 0) {
                needUpdate = true;
                for (FileStatus file : logFileList) {
                    // 如果是文件夹则将文件夹下的所有文件导入hive
                    if (file.isDirectory()) {
                        FileStatus[] subFileList = hdfs.listStatus(file.getPath());
                        if (subFileList == null || subFileList.length == 0) {
                            continue;
                        }
                        // 生成hive识别的hdfs路径
                        String hiveLogPath = new StringBuilder()
                                .append(hdfsInternalUri)
                                .append(hdfsRootLogPath)
                                .append(file.getPath().getName())
                                .toString();
                        // 导入数据
                        importData(stmt, hiveLogPath, tableName, false);
                    }
                }
            } else {
                System.out.println("文件夹[" + hdfsLogPath + "]下文件为空,无需导入");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            Client.close(hdfs);
            Client.close(stmt);
        }
    }
}
