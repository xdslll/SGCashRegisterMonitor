package com.sg.cash.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author xiads
 * @date 2018/3/30
 * @since
 */
public class HbaseUtil {

    //命名空间
    private static final String NS_NAME = "hbase20";
    //表名
    private static final String TABLE_NAME = "actor";
    //列簇名称
    private static final String COLUMN_FAMILY_NAME = "info";
    //完整表名
    private static final String FULL_TABLE_NAME = NS_NAME + ":" + TABLE_NAME;
    //行键
    private static final String ROW_KEY = "10010";

    private static final String COLUMN_1 = "name";
    private static final String VALUE_1 = "张三";
    private static final String COLUMN_2 = "age";
    private static final String VALUE_2 = "34";

    /**
     * 创建命名空间，创建表和列簇
     *
     * @throws IOException
     */
    public void test1CreateTable() throws IOException {
        //创建配置文件
        Configuration conf = HBaseConfiguration.create();
        //创建管理员
        HBaseAdmin admin = new HBaseAdmin(conf);
        //创建命名空间描述器
        NamespaceDescriptor nsDesc = NamespaceDescriptor.create(NS_NAME).build();
        //创建表格名称
        TableName tableName = TableName.valueOf(FULL_TABLE_NAME);
        //创建表格描述器
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        //创建列簇描述器
        HColumnDescriptor columnDesc = new HColumnDescriptor(COLUMN_FAMILY_NAME);
        //添加列簇
        tableDesc.addFamily(columnDesc);
        //管理员创建命名空间
        admin.createNamespace(nsDesc);
        //管理员创建表格
        admin.createTable(tableDesc);

        //重新获取表信息
        tableDesc = admin.getTableDescriptor((FULL_TABLE_NAME).getBytes());
        //验证表名
        // Assert.assertEquals(new String(tableDesc.getName()), FULL_TABLE_NAME);
        //验证列簇
        // Assert.assertEquals(new String(tableDesc.getColumnFamilies()[0].getName()), COLUMN_FAMILY_NAME);
    }

    /**
     * 测试插入数据
     *
     * @throws IOException
     */
    public void test2PutData() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, FULL_TABLE_NAME);
        Put put = new Put(toBytes(ROW_KEY));
        HColumnDescriptor[] colFamilies = table.getTableDescriptor().getColumnFamilies();
        for (int i = 0; i < colFamilies.length; i++) {
            String colFamName = colFamilies[i].getNameAsString();
            if (colFamName.equals(COLUMN_FAMILY_NAME)) {
                put.add(toBytes(colFamName), toBytes(COLUMN_1), toBytes(VALUE_1));
                put.add(toBytes(colFamName), toBytes(COLUMN_2), toBytes(VALUE_2));
            }
        }
        table.put(put);

        //验证数据插入结果
        Get get = new Get(toBytes(ROW_KEY));
        Result result = table.get(get);
        Cell[] rawCells = result.rawCells();

        //扫描所有数据
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            //assertCells(r.rawCells());
        }
    }

    /**
     * 删除数据
     *
     * @throws IOException
     */
    public void deleteData() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, FULL_TABLE_NAME);
        Delete delete = new Delete(toBytes(ROW_KEY));
        table.delete(delete);

        //验证数据删除结果
        Get get = new Get(toBytes(ROW_KEY));
        Result result = table.get(get);
        Cell[] rawCells = result.rawCells();
        // Assert.assertTrue(rawCells.length == 0);
    }

    /**
     * 禁用表、删除表、删除命名空间
     *
     * @throws IOException
     */
    public void dropTable() throws IOException {
        //创建配置文件
        Configuration conf = HBaseConfiguration.create();
        //创建管理员
        HBaseAdmin admin = new HBaseAdmin(conf);
        //获取表是否存在
        HTableDescriptor tableDesc = admin.getTableDescriptor((FULL_TABLE_NAME).getBytes());
        // Assert.assertNotNull(tableDesc);

        //禁用表
        admin.disableTable(FULL_TABLE_NAME);
        //验证表是否被禁用
        // Assert.assertTrue(admin.isTableDisabled(FULL_TABLE_NAME));

        //删除表
        admin.deleteTable(FULL_TABLE_NAME);
        //验证表是否被删除
        // Assert.assertTrue(!admin.isTableAvailable(FULL_TABLE_NAME));

        //删除命名空间
        admin.deleteNamespace(NS_NAME);

        NamespaceDescriptor nsDesc = null;
        try {
            nsDesc = admin.getNamespaceDescriptor(NS_NAME);
        } catch(Exception ex) {}
        //验证命名空间是否被删除
        // Assert.assertTrue(nsDesc == null);
    }

    private void assertCells(Cell[] rawCells) {
        for (int i = 0; i < rawCells.length; i++) {
            Cell cell = rawCells[i];
            String getColFamName = toString(CellUtil.cloneFamily(cell));
            String getColName = toString(CellUtil.cloneQualifier(cell));
            String getValue = toString(CellUtil.cloneValue(cell));
            if (i == 0) {
                // Assert.assertEquals(getColFamName, COLUMN_FAMILY_NAME);
                // Assert.assertEquals(getColName, COLUMN_2);
                // Assert.assertEquals(getValue, VALUE_2);
            } else {
                // Assert.assertEquals(getColFamName, COLUMN_FAMILY_NAME);
                // Assert.assertEquals(getColName, COLUMN_1);
                // Assert.assertEquals(getValue, VALUE_1);
            }
        }
    }

    public static byte[] toBytes(String s) {
        return Bytes.toBytes(s);
    }

    public static String toString(byte[] b) {
        return Bytes.toString(b);
    }
}
