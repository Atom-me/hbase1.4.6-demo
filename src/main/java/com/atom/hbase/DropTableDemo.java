package com.atom.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * drop table
 *
 * @author Atom
 */
public class DropTableDemo {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","10.16.118.247");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        //通过连接工厂创建连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);

        //通过连接获取管理员对象
        Admin admin = connection.getAdmin();
        admin.disableTable(TableName.valueOf("t1"));
        admin.deleteTable(TableName.valueOf("t1"));
        System.err.println("delete success");

        admin.close(); // contains connection.close();


    }
}
