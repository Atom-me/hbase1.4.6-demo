package com.atom.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * create new namespace
 *
 * @author Atom
 */
public class CreateNamespaceDemo {
    public static void main(String[] args) throws IOException {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","10.16.118.247");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        //通过连接工厂创建连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);

        //通过连接获取管理员对象
        Admin admin = connection.getAdmin();
        admin.createNamespace(NamespaceDescriptor.create("ns1").build());
        System.err.println("create namespace success");
        admin.close();

        /**
         * scan 查看创建结果
         * hbase(main):108:0> scan 'hbase:namespace'
         * ROW                                                          COLUMN+CELL
         *  default                                                     column=info:d, timestamp=1612162681738, value=\x0A\x07default
         *  hbase                                                       column=info:d, timestamp=1612162682887, value=\x0A\x05hbase
         *  ns1                                                         column=info:d, timestamp=1612244197901, value=\x0A\x03ns1
         * 3 row(s) in 0.1540 seconds
         */
    }
}
