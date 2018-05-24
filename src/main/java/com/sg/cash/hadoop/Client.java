package com.sg.cash.hadoop;

import com.sg.cash.hadoop.ftp.FtpUtil;
import com.sg.cash.hadoop.hdfs.HdfsUtil;
import com.sg.cash.hadoop.hive.HiveUtil;
import com.sg.cash.hadoop.sqlserver.SQLServerHandler;
import com.sg.cash.hadoop.sqoop.SqoopUtil;
import com.sg.cash.local.LocalLogFileHandler;
import com.sg.cash.util.ConfigUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author xiads
 * @date 2018/4/17
 * @since
 */
public class Client {

    /**
     * ftp目录
     */
    public static final String FTP_PATH = ConfigUtil.get("ftp_path");

    /**
     * 本地文件路径(gbk)
     */
    public static final String LOCAL_PATH = ConfigUtil.get("local_path");

    /**
     * 本地文件路径(utf-8)
     */
    public static final String LOCAL_PATH2 = ConfigUtil.get("local_path2");

    /**
     * hdfs远程uri
     */
    public static final String HDFS_REMOTE_URI = ConfigUtil.get("hdfs_remote_uri");
    public static final String HDFS_REMOTE_URI2 = ConfigUtil.get("hdfs_remote_uri2");

    /**
     * hdfs内部uri
     */
    public static final String HDFS_INTERNAL_URI = "hdfs://ns1";

    /**
     * hdfs用户名
     */
    public static final String HDFS_USER = ConfigUtil.get("hdfs_user");

    /**
     * hdfs上传根文件夹
     */
    public static final String HDFS_REPORT_DIR = ConfigUtil.get("hdfs_report_dir");

    /**
     * hdfs上传文件夹(实际存放文件路径)
     */
    public static final String HDFS_REPORT_INPUT_DIR = ConfigUtil.get("hdfs_report_input_dir");

    /**
     * hive连接字符串
     */
    public static final String HIVE_URL = ConfigUtil.get("hive_url");

    /**
     * hive连接用户
     */
    public static final String HIVE_USER = ConfigUtil.get("hive_user");

    /**
     * hive连接密码
     */
    public static final String HIVE_PASSWORD = ConfigUtil.get("hive_password");

    /**
     * hive数据库名
     */
    public static final String HIVE_DB = ConfigUtil.get("hive_db");

    /**
     * hive日志表名
     */
    public static final String HIVE_TABLE_LOG = ConfigUtil.get("hive_table_log");

    /**
     * hive门店表名
     */
    public static final String HIVE_TABLE_STORE = ConfigUtil.get("hive_table_store");

    /**
     * hive收银机表名
     */
    public static final String HIVE_TABLE_MACHINE = ConfigUtil.get("hive_table_machine");

    /**
     * hive仓库地址
     */
    public static final String HIVE_WAREHOUSE = ConfigUtil.get("hive_warehouse");
    // public static final String HIVE_WAREHOUSE2 = "/user/hive/warehouse/sg.db/tbl_sg_report_cash_detail/";

    /**
     * 门店相关的文件
     */
    public static final String HDFS_UPLOAD_STORE_DIR = ConfigUtil.get("hdfs_upload_store_dir");
    public static final String LOCAL_ORIGINAL_STORE_FILE_PATH = "/Users/apple/Desktop/store.csv";
    public static final String LOCAL_NEW_STORE_FILE_PATH = "/Users/apple/Desktop/store2.csv";
    public static final String LOCAL_FINAL_STORE_FILE_PATH = "/Users/apple/Desktop/store3.csv";

    /**
     * 收银机相关的文件
     */
    public static final String HDFS_UPLOAD_MACHINE_DIR = ConfigUtil.get("hdfs_upload_machine_dir");
    public static final String LOCAL_CASH_MACHINE_FILE_PATH = ConfigUtil.get("local_cash_machine_file_path");

    /**
     * MySQL相关参数
     */
    public static final String MYSQL_URL = "jdbc:mysql://www.mac-bigdata-01.com:3306/sg?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PASSWORD = "123456";
    public static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";

    /**
     * sqoop相关配置
     */
    public static final String SQOOP_MYSQL_URL = "jdbc:mysql://www.mac-bigdata-01.com:3306/sg";
    public static final String SQOOP_TABLE_NAME = "result_avg_cash_time";
    public static final String SQOOP_TABLE_NAME2 = "result_avg_machine_effective";
    public static final String SQOOP_HIVE_WAREHOUSE = "/user/hive/warehouse/sg2.db/result_avg_cash_time";

    /**
     * SQLServer IP
     */
    public static final String SQL_SERVER_IP = ConfigUtil.get("sql_server_ip");

    /**
     * SQLServer端口号
     */
    public static final int SQL_SERVER_PORT = ConfigUtil.getInt("sql_server_port");

    /**
     * SQLServer用户名
     */
    public static final String SQL_SERVER_USER = ConfigUtil.get("sql_server_user");

    /**
     * SQLServer密码
     */
    public static final String SQL_SERVER_PWD = ConfigUtil.get("sql_server_pwd");

    /**
     * SQLServer JDBC驱动名
     */
    public static final String SQL_SERVER_DRIVER_NAME = ConfigUtil.get("sql_server_driver_name");

    /**
     * SQLServer连接字符串
     */
    public static final String SQL_SERVER_DB_URL = "jdbc:sqlserver://" + SQL_SERVER_IP + ":"
            + SQL_SERVER_PORT;

    public static void main(String[] args) {
        if (args.length < 1) {
            showHelp();
            throw new RuntimeException("参数不能为空");
        }
        long _START = System.currentTimeMillis();
        String type = args.length >=1 ? args[0] : "";
        String cmd = args.length >= 2 ? args[1] : "";
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println("Current time:" + df.format(new Date()));
        System.out.println("Your command is: " + type + " " + cmd);
        if (type.equals("ftp")) {
            if (cmd.equals("check")) {
                ftpCheck();
            } else if (cmd.equals("sync")) {
                ftpSync();
            } else if ((cmd.equals("check-zero"))) {
                ftpCheckZero();
            }
        } else if (type.equals("local")) {
            if (cmd.equals("check")) {
                localCheck();
            } else if (cmd.equals("convert")) {
                localConvert();
            }
        } else if (type.equals("hdfs")) {
            if (cmd.equals("check")) {
                hdfsCheck();
            } else if (cmd.equals("upload")) {
                hdfsUpload();
            }
        } else if (type.equals("hive")) {
            if (cmd.equals("check")) {
                hiveCheck();
            } else if (cmd.equals("upload")) {
                hiveUpload();
            }
        } else if (type.equals("hupu")) {
            if (cmd.equals("check")) {
                checkHupu();
            } else if (cmd.equals("sync")) {
                syncHupu();
            }
        } else if (type.equals("help")) {
            showHelp();
        } else {
            System.out.println("Command cannot be recognized!");
            showHelp();
        }
        long _END = System.currentTimeMillis();
        System.out.println("elapsed time:" + (double) (_END - _START) / 1000 + "s");
    }

    private static void showHelp() {
        System.out.println("You can use following command...");
        System.out.println("- ftp");
        System.out.println("  > check : show ftp status");
        System.out.println("  > check-zero : show number of zero files");
        System.out.println("  > sync : sync data from ftp");
        System.out.println("- local");
        System.out.println("  > check : check status of local files");
        System.out.println("  > convert : convert file encoding from gbk to utf-8");
        System.out.println("- hdfs");
        System.out.println("  > check : show hdfs status");
        System.out.println("  > upload : upload files to hdfs");
        System.out.println("- hive");
        System.out.println("  > check : show hive status");
        System.out.println("  > upload : import files to hive");
        System.out.println("- hupu");
        System.out.println("  > check : show hupu databases");
        System.out.println("  > sync : sync data from hupu databases");
    }

    private static void syncHupu() {
        SQLServerHandler.syncHupuData();
    }

    private static void checkHupu() {
        Connection conn = null;
        SQLServerHandler handler = new SQLServerHandler(SQL_SERVER_DRIVER_NAME, SQL_SERVER_DB_URL, SQL_SERVER_USER, SQL_SERVER_PWD);
        try {
            System.out.println("正在连接SQLServer...");
            conn = handler.connect();
            System.out.println("SQLServer连接成功!");
            System.out.println(SQL_SERVER_DB_URL);
            handler.checkHupuDbNames(conn);
        } catch(Exception ex) {
            ex.printStackTrace();
        } finally {
            close(conn);
        }

    }

    private static void hiveCheck() {
        Connection conn = null;
        try {
            HiveUtil hiveUtil = new HiveUtil(HIVE_URL, HIVE_USER, HIVE_PASSWORD);
            conn = hiveUtil.connect();
            hiveUtil.checkDb(conn, HIVE_DB);
            hiveUtil.checkTable(conn, HIVE_DB, HIVE_TABLE_LOG);
            hiveUtil.checkTable(conn, HIVE_DB, HIVE_TABLE_STORE);
            hiveUtil.checkTable(conn, HIVE_DB, HIVE_TABLE_MACHINE);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            close(conn);
        }
    }

    private static void hiveUpload() {
        if (!HiveUtil.uploadToHiveWarehouse()) {
            System.out.println("上传hive仓库出错!");
        } else {
            System.out.println("上传hive仓库成功!");
        }
    }

    private static void hdfsCheck() {
        String activeHdfsRemoteUri = HdfsUtil.checkActiveHdfs(HDFS_REMOTE_URI, HDFS_REMOTE_URI2, HDFS_USER);
        System.out.println("当前激活的hdfs链接为:" + activeHdfsRemoteUri);
        HdfsUtil.check(activeHdfsRemoteUri, HDFS_USER, HDFS_REPORT_INPUT_DIR);
        HdfsUtil.check(activeHdfsRemoteUri, HDFS_USER, HIVE_WAREHOUSE);
        HdfsUtil.check(activeHdfsRemoteUri, HDFS_USER, HDFS_UPLOAD_STORE_DIR);
        HdfsUtil.check(activeHdfsRemoteUri, HDFS_USER, HDFS_UPLOAD_MACHINE_DIR);
    }

    private static void hdfsUpload() {
        String activeHdfsRemoteUri = HdfsUtil.checkActiveHdfs(HDFS_REMOTE_URI, HDFS_REMOTE_URI2, HDFS_USER);
        System.out.println("当前激活的hdfs链接为:" + activeHdfsRemoteUri);
        if (!HdfsUtil.uploadFileToHdfs(activeHdfsRemoteUri, HDFS_USER, HDFS_REPORT_DIR, LOCAL_PATH2, HIVE_WAREHOUSE)) {
            System.out.println("上传日志文件到hdfs出错!");
        }

        if (!HdfsUtil.uploadStoreFileToHdfs(activeHdfsRemoteUri, HDFS_USER, HDFS_UPLOAD_STORE_DIR,
                LOCAL_ORIGINAL_STORE_FILE_PATH, LOCAL_NEW_STORE_FILE_PATH, LOCAL_FINAL_STORE_FILE_PATH)) {
            System.out.println("上传门店文件到hdfs出错!");
        }

        if (!HdfsUtil.uploadMachineFileToHdfs(activeHdfsRemoteUri, HDFS_USER, HDFS_UPLOAD_MACHINE_DIR,
                LOCAL_CASH_MACHINE_FILE_PATH)) {
            System.out.println("上传门店文件到hdfs出错!");
        }
    }

    private static void localCheck() {
        LocalLogFileHandler.check(LOCAL_PATH);
        LocalLogFileHandler.check(LOCAL_PATH2);
    }

    private static void localConvert() {
        if (!LocalLogFileHandler.convert(LOCAL_PATH, LOCAL_PATH2, false)) {
            System.out.println("本地数据转码出错!");
        } else {
            System.out.println("本地数据转码成功!");
        }
    }

    public static void ftpCheck() {
        FtpUtil.check(ConfigUtil.get("ftp_path"));
    }

    private static void ftpCheckZero() {
        FtpUtil.checkZero(ConfigUtil.get("ftp_path"));
    }

    public static void ftpSync() {
        if (!FtpUtil.sync(FTP_PATH, LOCAL_PATH)) {
            System.out.println("ftp数据同步出错!");
        } else {
            System.out.println("ftp数据同步成功!");
        }
    }

    public static void check() {
        //FtpUtil.check(FTP_PATH);
        //LocalLogFileHandler.check(LOCAL_PATH);
        //LocalLogFileHandler.check(LOCAL_PATH2);
        //HdfsUtil.check(HDFS_REMOTE_URI, HDFS_USER, HDFS_REPORT_INPUT_DIR);
        //HdfsUtil.check(HDFS_REMOTE_URI, HDFS_USER, HIVE_WAREHOUSE);
        // 测试代码
        //HdfsUtil.compareDirectories(HDFS_REMOTE_URI, HDFS_USER, HIVE_WAREHOUSE, HIVE_WAREHOUSE2);
        //HdfsUtil.download(HDFS_REMOTE_URI, HDFS_USER, "hdfs://www.mac-bigdata-01.com:8020/user/hive/warehouse/sg2.db/tbl_sg_report_cash_detail/0319_0012_20180306.log", "/Users/apple/Desktop/0319_0012_20180306_1.log");
        //HdfsUtil.download(HDFS_REMOTE_URI, HDFS_USER, "hdfs://www.mac-bigdata-01.com:8020/user/hive/warehouse/sg.db/tbl_sg_report_cash_detail/0319_0012_20180306.log", "/Users/apple/Desktop/0319_0012_20180306_2.log");
    }

    public static void sync() {
        /*// 同步ftp数据
        if (!FtpUtil.sync(FTP_PATH, LOCAL_PATH)) {
            System.out.println("ftp服务同步出错!");
            return;
        }*/

        // 将本地数据进行转码(gbk->utf8)
        /*if (!LocalLogFileHandler.convert(LOCAL_PATH, LOCAL_PATH2, false)) {
            System.out.println("本地数据转码出错!");
            return;
        }

        // 将本地文件上传至hdfs
        if (!HdfsUtil.uploadFileToHdfs(HDFS_REMOTE_URI, HDFS_USER, HDFS_REPORT_DIR, LOCAL_PATH2, HIVE_WAREHOUSE)) {
            System.out.println("上传日志文件到hdfs出错!");
            return;
        }

        if (!HdfsUtil.uploadStoreFileToHdfs(HDFS_REMOTE_URI, HDFS_USER, HDFS_UPLOAD_STORE_DIR,
                LOCAL_ORIGINAL_STORE_FILE_PATH, LOCAL_NEW_STORE_FILE_PATH, LOCAL_FINAL_STORE_FILE_PATH)) {
            System.out.println("上传门店文件到hdfs出错!");
            return;
        }

        if (!HdfsUtil.uploadMachineFileToHdfs(HDFS_REMOTE_URI, HDFS_USER, HDFS_UPLOAD_MACHINE_DIR,
                LOCAL_CASH_MACHINE_FILE_PATH)) {
            System.out.println("上传门店文件到hdfs出错!");
            return;
        }*/

        // 将hdfs数据导入hive
        if (!HiveUtil.uploadToHiveWarehouse()) {
            System.out.println("上传hive仓库出错!");
            return;
        }
        // 将hive数据导入mysql
        if (!SqoopUtil.clearTable(Client.SQOOP_TABLE_NAME)) {
            System.out.println("清空数据表[" + Client.SQOOP_TABLE_NAME + "]失败!");
            return;
        }
        if (!SqoopUtil.exportDataToMySQLByCmd("/opt/sg/sqoop_export_avg_time.file")) {
            System.out.println("hive导出sqoop_export_avg_time.file出错!");
            return;
        }
        if (!SqoopUtil.clearTable(Client.SQOOP_TABLE_NAME2)) {
            System.out.println("清空数据表[" + Client.SQOOP_TABLE_NAME + "]失败!");
            return;
        }
        if (!SqoopUtil.exportDataToMySQLByCmd("/opt/sg/sqoop_export_avg_effective.file")) {
            System.out.println("hive导出sqoop_export_avg_effective.file出错!");
            return;
        }

        // 自动导入互普的数据
        SQLServerHandler.syncHupuData();
    }

    public static void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
