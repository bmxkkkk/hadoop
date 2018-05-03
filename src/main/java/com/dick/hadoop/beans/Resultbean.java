package com.dick.hadoop.beans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Writable;

public class Resultbean implements Writable {
	@Override
	public String toString() {
		 Set<Entry<String, Long>> set = this.map.entrySet();
		 StringBuffer sb =  new StringBuffer();
		 for (Entry<String, Long> entry : set) {
			sb.append(entry.getKey());
			sb.append("-->");
			sb.append(entry.getValue());
			sb.append(" ");
		}
		return sb.toString();
	}

	private String name;
	private Map<String,Long> map;
	private String mapString;
	public Resultbean() {
		
	}
	public Resultbean(String name,Map<String,Long> map ) {
		this.name = name;
		this.map = map;

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeUTF(this.toString());
	
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name = in.readUTF();
		this.mapString = in.readUTF();
		String[] strArr = this.mapString.split(" ");
		for (String str : strArr) {
			String[] ss = str.split("-->");
			this.map =  new HashMap<String,Long>();
			this.map.put(ss[0], Long.parseLong(ss[1]));
		}
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Map<String, Long> getMap() {
		return map;
	}
	public void setMap(Map<String, Long> map) {
		this.map = map;
	}
	public String getMapString() {
		return mapString;
	}
	public void setMapString(String mapString) {
		this.mapString = mapString;
	}
	

	
}
