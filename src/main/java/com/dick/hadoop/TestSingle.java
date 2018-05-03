package com.dick.hadoop;

public class TestSingle {
	static {
		
		System.out.println("加载！");
	} 
public static void  pppp() {
	System.out.println("调用方法");
}
 public static void main(String[] args) {
	 //Te = stSinglet = .pppp();

	 TestSingle.Ss.pppp();;
}
 public static class Ss{
	 static {
		 System.out.println("内部类加载");
	 }
	 public static void  pppp() {
			System.out.println("调用方法");
		}
 }
}
