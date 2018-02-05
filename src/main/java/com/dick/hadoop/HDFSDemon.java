package com.dick.hadoop;

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
public class HDFSDemon 
{
    public static void main( String[] args ) throws IOException, URISyntaxException
    {	
    	FileSystem  fs =  FileSystem.get(new URI("hdfs://eshoop-cache01:9000"), new Configuration());
    	InputStream in = fs.open(new Path("/words"));
    	OutputStream out  =  new FileOutputStream("D://words");
    	System.out.println();
    	IOUtils.copyBytes(in, out, 4096,true);
    }
}
