package com.dick.hadoop;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import com.dick.hadoop.protocal.Protocal;

public class RPCClient {
	public static void main(String[] args) throws IOException {
		Protocal proxy =  RPC.getProxy(Protocal.class, 10010, new InetSocketAddress("192.168.1.114",9527), new Configuration());
		proxy.sayHello();
	}
}
