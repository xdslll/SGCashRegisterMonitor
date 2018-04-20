package com.sg.cash.hadoop;

import com.sg.cash.hadoop.ftp.FtpUtil;
import com.sg.cash.hadoop.hdfs.HdfsUtil;
import com.sg.cash.hadoop.hive.HiveUtil;
import com.sg.cash.hadoop.sqoop.SqoopUtil;
import com.sg.cash.local.LocalLogFileHandler;

/**
 * @author xiads
 * @date 2018/4/17
 * @since
 */
public class Client {

    /**
     * ftp目录
     */
    public static final String FTP_PATH = "/logs/JKreport/";

    /**
     * 本地文件路径(gbk)
     */
    public static final String LOCAL_PATH = "/Users/apple/Desktop/test/";

    /**
     * 本地文件路径(utf-8)
     */
    public static final String LOCAL_PATH2 = "/Users/apple/Desktop/test2/";

    /**
     * hdfs远程uri
     */
    public static final String HDFS_REMOTE_URI = "hdfs://www.mac-bigdata-02.com:8020";

    /**
     * hdfs内部uri
     */
    public static final String HDFS_INTERNAL_URI = "hdfs://ns1";

    /**
     * hdfs用户名
     */
    public static final String HDFS_USER = "hadoop";

    /**
     * hdfs上传根文件夹
     */
    public static final String HDFS_REPORT_DIR = "/sg/report/";

    /**
     * hdfs上传文件夹(实际存放文件路径)
     */
    public static final String HDFS_REPORT_INPUT_DIR = "/sg/report/input/";

    /**
     * hive连接字符串
     */
    public static final String HIVE_URL = "jdbc:hive2://www.mac-bigdata-01.com:10000/sg";

    /**
     * hive连接用户
     */
    public static final String HIVE_USER = "hadoop";

    /**
     * hive连接密码
     */
    public static final String HIVE_PASSWORD = "123456";

    /**
     * hive数据库名
     */
    public static final String HIVE_DB = "sg2";

    /**
     * hive日志表名
     */
    public static final String HIVE_TABLE_LOG = "tbl_sg_report_cash_detail";

    /**
     * hive门店表名
     */
    public static final String HIVE_TABLE_STORE = "tbl_sg_store_info";

    /**
     * hive仓库地址
     */
    public static final String HIVE_WAREHOUSE = "/user/hive/warehouse/sg2.db/tbl_sg_report_cash_detail/";
    public static final String HIVE_WAREHOUSE2 = "/user/hive/warehouse/sg.db/tbl_sg_report_cash_detail/";

    /**
     * 门店相关的文件
     */
    public static final String HDFS_UPLOAD_STORE_DIR = "/sg/report/store/";
    public static final String LOCAL_ORIGINAL_STORE_FILE_PATH = "/Users/apple/Desktop/store.csv";
    public static final String LOCAL_NEW_STORE_FILE_PATH = "/Users/apple/Desktop/store2.csv";
    public static final String LOCAL_FINAL_STORE_FILE_PATH = "/Users/apple/Desktop/store3.csv";

    /**
     * MySQL相关参数
     */
    public static final String MYSQL_URL = "jdbc:mysql://www.mac-bigdata-01.com:3306/sg?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull";
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PASSWORD = "123456";
    public static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";

    public static final String SQOOP_MYSQL_URL = "jdbc:mysql://www.mac-bigdata-01.com:3306/sg";
    public static final String SQOOP_TABLE_NAME = "result_avg_cash_time";
    public static final String SQOOP_HIVE_WAREHOUSE = "/user/hive/warehouse/sg2.db/result_avg_cash_time";

    public static void main(String[] args) {
        long _START = System.currentTimeMillis();

        // 同步ftp数据
        if (!FtpUtil.sync(FTP_PATH, LOCAL_PATH)) {
            System.out.println("ftp服务同步出错!");
            return;
        }
        // 将本地数据进行转码(gbk->utf8)
        if (!LocalLogFileHandler.convert(LOCAL_PATH, LOCAL_PATH2, false)) {
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
        // 将hdfs数据导入hive
        if (!HiveUtil.uploadToHiveWarehouse()) {
            System.out.println("上传hive仓库出错!");
            return;
        }
        // 将hive数据导入mysql
        if (!SqoopUtil.exportDataToMySQLByCmd()) {
            System.out.println("hive导出mysql出错!");
            return;
        }
        // 测试代码
        //HdfsUtil.compareDirectories(HDFS_REMOTE_URI, HDFS_USER, HIVE_WAREHOUSE, HIVE_WAREHOUSE2);
        //HdfsUtil.download(HDFS_REMOTE_URI, HDFS_USER, "hdfs://www.mac-bigdata-01.com:8020/user/hive/warehouse/sg2.db/tbl_sg_report_cash_detail/0319_0012_20180306.log", "/Users/apple/Desktop/0319_0012_20180306_1.log");
        //HdfsUtil.download(HDFS_REMOTE_URI, HDFS_USER, "hdfs://www.mac-bigdata-01.com:8020/user/hive/warehouse/sg.db/tbl_sg_report_cash_detail/0319_0012_20180306.log", "/Users/apple/Desktop/0319_0012_20180306_2.log");
        // 将hive结果集导入mysql
        long _END = System.currentTimeMillis();
        System.out.println("elapsed time:" + (double) (_END - _START) / 1000 + "s");
    }
}
