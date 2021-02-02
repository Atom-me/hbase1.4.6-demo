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
 * update
 *
 * @author Atom
 */
public class Put4UpdateDemo {
    public static void main(String[] args) throws IOException {

        //创建配置对象,, load the hdfs-site.xml
        //hdfs-site.xml 里面只需要配置zk信息就可以，所有的信息都在zk里面
        Configuration configuration = HBaseConfiguration.create();
        //通过连接工厂创建连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);

        // use hbase shell create table: create 'ns1:t1', {NAME => 'cf1'}
        //通过连接获取table信息
        Table table = connection.getTable(TableName.valueOf("ns1:t1"));

        //设定row 信息
        Put put = new Put(Bytes.toBytes("row1"));
        //设置字段值
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("atom"));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("city"), Bytes.toBytes("hainan"));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes("19"));

        // put
        table.put(put);

        System.err.println(" update data success");

        //release
        table.close(); //  contains connection.close();


        /**
         * //验证： use hbase shell: get 'ns1:t1'
         *before:
         *hbase(main):114:0> get 'ns1:t1','row1'
         * COLUMN                                                       CELL
         *  cf1:age                                                     timestamp=1612245230543, value=19
         *  cf1:city                                                    timestamp=1612245230543, value=beijing
         *  cf1:name                                                    timestamp=1612245230543, value=tom
         * 1 row(s) in 0.5510 seconds
         *
         * after:
         *
         * hbase(main):115:0> get 'ns1:t1','row1'
         * COLUMN                                                       CELL
         *  cf1:age                                                     timestamp=1612245440089, value=19
         *  cf1:city                                                    timestamp=1612245440089, value=hainan
         *  cf1:name                                                    timestamp=1612245440089, value=atom
         * 1 row(s) in 0.2450 seconds
         */


    }
}
