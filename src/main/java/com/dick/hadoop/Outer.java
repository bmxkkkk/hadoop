package com.dick.hadoop;
public class Outer
{
		int out_x  = 0;
		public void method()
		{
			Inner1 inner1 = new Inner1();
			class Inner2   //在方法体内部定义的内部类
			{
				public void method()
				{
					out_x = 3;
				}
			}
			Inner2 inner2 = new Inner2();
		}

		 class Inner1   //在方法体外面定义的内部类
		{
		}
		 public static void main(String[] args) {
			 Outer o = new Outer();
			 Outer.Inner1 i = o.new Inner1();
		}
		
}
