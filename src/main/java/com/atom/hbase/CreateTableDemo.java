package com.atom.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * create table
 *
 * @author Atom
 */
public class CreateTableDemo {
    public static void main(String[] args) throws IOException {

        Configuration configuration = HBaseConfiguration.create();
        //通过连接工厂创建连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);

        //通过连接获取管理员对象
        Admin admin = connection.getAdmin();
        //表描述符
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf("ns1:t1"));
        //列族描述符
        HColumnDescriptor columnDesc = new HColumnDescriptor("cf1");
        tableDesc.addFamily(columnDesc);
        admin.createTable(tableDesc);
        System.err.println("create table success");
        admin.close();
    }
}
