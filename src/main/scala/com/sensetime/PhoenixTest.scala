package com.sensetime

import java.security.PrivilegedExceptionAction
import java.sql.DriverManager

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}

object PhoenixTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")


    val conf = new SparkConf().setAppName("SparkSecurePhoenix")
    val sc = new SparkContext(conf)

    val hbaseconf = HBaseConfiguration.create;
    hbaseconf.set("hadoop.security.authentication", "Kerberos");

    UserGroupInformation.setConfiguration(hbaseconf);

    val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hatieda@HADOOP-VIDEO.DATA.SENSETIME.COM", "/home/hatieda/hatieda.hadoop-video.keytab");
    UserGroupInformation.setLoginUser(ugi)
    ugi.doAs(new PrivilegedExceptionAction[Void]() {

      override def run: Void = {

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

        val url = "jdbc:phoenix:node-01:2181/hbasebig/hbase";
        val conn = DriverManager.getConnection(url);
        val prepare = conn.prepareStatement("select * from test.person")
        val rs = prepare.executeQuery()
        while (rs.next()) {
          println(rs.getString(1))
        }


        null
      }
    })

    sc.stop()

  }

}
