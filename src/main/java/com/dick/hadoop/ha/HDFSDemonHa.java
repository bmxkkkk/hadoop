package com.dick.hadoop.ha;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Hello world!
 *
 */
public class HDFSDemonHa 
{
    public static void main( String[] args ) throws IOException, URISyntaxException
    {	
    	Configuration cfg =  new Configuration();
    	cfg.set("dfs.nameservices","ns1");
    	cfg.set("dfs.ha.namenodes.ns1", "nn1,nn2");
    	cfg.set("dfs.namenode.rpc-address.ns1.nn1", "eshoop-cache01:9000");
    	cfg.set("dfs.namenode.rpc-address.ns1.nn2", "eshoop-cache02:9000");
    	cfg.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
    	FileSystem  fs =  FileSystem.get(new URI("hdfs://ns1"), cfg);
    	InputStream in = fs.open(new Path("/a.txt"));
    	OutputStream out  =  new FileOutputStream("D://a.txt");
    	IOUtils.copyBytes(in, out, 4096,true);
    }
}
