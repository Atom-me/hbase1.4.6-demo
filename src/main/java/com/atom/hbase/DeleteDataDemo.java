package com.atom.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * delete data
 * <p>
 * 删除是删除列族中的某一列
 *
 * @author Atom
 */
public class DeleteDataDemo {
    public static void main(String[] args) throws IOException {

        //创建配置对象,, load the hdfs-site.xml
        //hdfs-site.xml 里面只需要配置zk信息就可以，所有的信息都在zk里面
        Configuration configuration = HBaseConfiguration.create();
        //通过连接工厂创建连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);

        // use hbase shell create table: create 'ns1:t1', {NAME => 'cf1'}
        //通过连接获取table信息
        Table table = connection.getTable(TableName.valueOf("ns1:t1"));

        Delete delete = new Delete(Bytes.toBytes("row80"));
        delete.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
        table.delete(delete);

        System.err.println(" delete column success");

        //release
        table.close(); //  contains connection.close();


    }
}
