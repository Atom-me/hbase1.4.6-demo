package com.atom.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * insert
 *
 * @author Atom
 */
public class Put4InsertDemo {
    public static void main(String[] args) throws IOException {

        //创建配置对象,, load the hdfs-site.xml
        //hdfs-site.xml 里面只需要配置zk信息就可以，所有的信息都在zk里面
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","10.16.118.247");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        //通过连接工厂创建连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);

        // use hbase shell create table: create 'ns1:t1', {NAME => 'cf1'}
        //通过连接获取table信息
        Table table = connection.getTable(TableName.valueOf("ns1:t1"));

        //设定row 信息
        Put put = new Put(Bytes.toBytes("row1"));
        //设置字段值
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("tom"));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("city"), Bytes.toBytes("beijing"));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes("19"));

        // put
        table.put(put);

        System.err.println(" put data success");

        //release
        table.close(); //  contains connection.close();


        //验证： use hbase shell: scan 'ns1:t1' 插入成功


    }
}
