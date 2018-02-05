package com.dick.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text k2, Iterable<LongWritable> v2s,
	Context context) throws IOException, InterruptedException {
		long counter = 0;
		for (LongWritable i : v2s) {
			counter+=i.get();
		}
		context.write(new Text(k2), new LongWritable(counter));
		
	}
	
	
}
