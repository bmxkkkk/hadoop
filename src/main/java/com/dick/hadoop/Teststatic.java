package com.dick.hadoop;

public class Teststatic {
	public static Integer x=1 ;
	static {
		x++;
	}
	public Teststatic() {
		x++;
		System.out.println(x);
	}
	public static void main(String[] args) {
		new Teststatic();
		new Teststatic();
		
	}
}
