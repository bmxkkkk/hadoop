package com.dick.hadoop;
public class VariantTest{
		public static int staticVar = 0; 
		
		public VariantTest(){
			staticVar++;
	
		}
		public static void main(String[] args) {
			new VariantTest();
			new VariantTest();
			System.out.println(VariantTest.staticVar);
		}
}
