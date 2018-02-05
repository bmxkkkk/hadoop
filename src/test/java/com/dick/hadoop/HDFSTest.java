package com.dick.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import org.junit.Before;
import org.junit.Test;


public class HDFSTest
{
	FileSystem  fs = null;
	@Before
	public void init() throws IOException, URISyntaxException, InterruptedException {  	
		  fs =  FileSystem.get(new URI("hdfs://eshoop-cache01:9000"), new Configuration(),"root");    
	}
	@Test
	public void upload() throws IllegalArgumentException, IOException {
		InputStream in =  new FileInputStream(new File("D://pygam_hello_8.py"));
		OutputStream out = fs.create(new Path("/pygam_hello.py"));
		IOUtils.copyBytes(in, out, 4096,true);
		
	}
	@Test
	public void download() throws IllegalArgumentException, IOException {
		fs.copyToLocalFile(true,new Path("/pygam_hello.py"), new Path("D://game.py"),true);
		
		
	}
}
