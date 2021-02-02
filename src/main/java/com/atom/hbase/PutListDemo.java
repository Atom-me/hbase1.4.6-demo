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
import java.util.ArrayList;
import java.util.List;

/**
 * batch insert
 *
 * @author Atom
 */
public class PutListDemo {
    public static void main(String[] args) throws IOException {

        //创建配置对象,, load the hdfs-site.xml
        //hdfs-site.xml 里面只需要配置zk信息就可以，所有的信息都在zk里面
        Configuration configuration = HBaseConfiguration.create();
        //通过连接工厂创建连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);

        // use hbase shell create table: create 'ns1:t1', {NAME => 'cf1'}
        //通过连接获取table信息
        Table table = connection.getTable(TableName.valueOf("ns1:t1"));

        List<Put> puts = new ArrayList<>();
        //设定row 信息
        Put put;
        for (int i = 0; i < 100; i++) {
            put = new Put(Bytes.toBytes("row" + i));
            //设置字段值
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("no"), Bytes.toBytes(i));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("atom-" + i));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("city"), Bytes.toBytes("beijing"));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(18 + i));
            puts.add(put);
        }


        // put
        table.put(puts);

        System.err.println(" put list data success");

        //release
        table.close(); //  contains connection.close();


        //验证： use hbase shell: scan 'ns1:t1' 批量插入成功


    }
}
