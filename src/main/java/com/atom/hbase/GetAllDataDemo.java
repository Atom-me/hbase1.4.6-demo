package com.atom.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * get all Data
 *
 *
 * <p>
 * scan
 * <p>
 * .withStartRow(Bytes.toBytes("row90"))
 * .withStopRow(Bytes.toBytes("row96"));
 *
 * @author Atom
 */
public class GetAllDataDemo {
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

//        Scan scan = new Scan();
        Scan scan = new Scan()
                .withStartRow(Bytes.toBytes("row90"))
                .withStopRow(Bytes.toBytes("row96"));
        scan.addFamily(Bytes.toBytes("cf1"));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            byte[] noByteValue = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("no"));
            byte[] ageByteValue = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
            byte[] cityByteValue = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("city"));
            byte[] nameByteValue = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
            int no = Bytes.toInt(noByteValue);
            int age = Bytes.toInt(ageByteValue);
            String city = Bytes.toString(cityByteValue);
            String name = Bytes.toString(nameByteValue);

            System.err.println(no + "," + name + "," + age + "," + city);
        }


        System.err.println("get all data success");

        //release
        table.close(); //  contains connection.close();


    }
}
