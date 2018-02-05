package com.dick.hadoop;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import com.dick.hadoop.protocal.Protocal;

public class RPCServer implements Protocal {
	public void sayHello() {
		System.out.println("hello");
		
	}
	public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
		Server server = new RPC.Builder(new Configuration()).
		setBindAddress("192.168.1.114").
		setPort(9527).
		setProtocol(Protocal.class).
		setInstance(new RPCServer()).
		build();
		server.start();
	}

	
}
