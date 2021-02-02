package com.atom.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * drop  namespace
 *
 * @author Atom
 */
public class DropNamespaceDemo {
    public static void main(String[] args) throws IOException {

        Configuration configuration = HBaseConfiguration.create();
        //通过连接工厂创建连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);

        //通过连接获取管理员对象
        Admin admin = connection.getAdmin();
        admin.deleteNamespace("ns1");
        System.err.println("delete  namespace success");
        admin.close();

        /**
         * scan 查看创建结果
         * hbase(main):109:0> scan 'hbase:namespace'
         * ROW                                                          COLUMN+CELL
         *  default                                                     column=info:d, timestamp=1612162681738, value=\x0A\x07default
         *  hbase                                                       column=info:d, timestamp=1612162682887, value=\x0A\x05hbase
         * 2 row(s) in 0.2850 seconds
         * */
    }
}
