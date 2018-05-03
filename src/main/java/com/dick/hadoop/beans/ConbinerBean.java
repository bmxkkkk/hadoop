package com.dick.hadoop.beans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ConbinerBean implements Writable {
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return this.name+"\t"+this.fileName+"\t"+this.count;
	}
	String name;	
	public ConbinerBean() {

	}
	public ConbinerBean(String name	, String filename, String count) {
		this.name = name;
		this.fileName = filename;
		this.count = count;
	}

	String fileName;
	String count;
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(fileName);
		out.writeUTF(count);
		
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		in.readUTF();
		in.readUTF();
		in.readUTF();
		
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getCount() {
		return count;
	}
	public void setCount(String count) {
		this.count = count;
	}
	
	
}
