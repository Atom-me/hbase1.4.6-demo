package com.atom.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Objects;

/**
 * get Data
 *
 * @author Atom
 */
public class GetDataDemo {
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
        Get get = new Get(Bytes.toBytes("row80"));
//        get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("no"));// *** only get the 'no' column
        get.addFamily(Bytes.toBytes("cf1"));
        Result result = table.get(get);

        byte[] noByteValue = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("no"));
        byte[] ageByteValue = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
        byte[] cityByteValue = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("city"));
        byte[] nameByteValue = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name"));

        int no = Objects.isNull(noByteValue) ? -1 : Bytes.toInt(noByteValue);
        int age = Objects.isNull(ageByteValue) ? -1 : Bytes.toInt(ageByteValue);
        String city = Objects.isNull(cityByteValue) ? "" : Bytes.toString(cityByteValue);
        String name = Objects.isNull(nameByteValue) ? "" : Bytes.toString(nameByteValue);

        System.err.println(no + "," + name + "," + age + "," + city);

        System.err.println("get data success");

        //release
        table.close(); //  contains connection.close();

    }
}
