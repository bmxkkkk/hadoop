package com.dick.hadoop.index.mr;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.dick.hadoop.beans.ConbinerBean;
import com.dick.hadoop.beans.Databean;
import com.dick.hadoop.beans.Resultbean;

public class InverseIndex {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
	Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		//设置jar
		job.setJarByClass(InverseIndex.class);
		
		//设置Mapper相关的属性
		job.setMapperClass(IndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Databean.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));//words.txt
		
		//设置Reducer相关属性
		job.setReducerClass(IndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Resultbean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setCombinerClass(IndexCombiner.class);	
		//提交任
		job.waitForCompletion(true);
	}
	
	public static class IndexMapper extends Mapper<LongWritable, Text, Text, Databean>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Databean>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(" ");
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			Path path = inputSplit.getPath();
			String name = path.getName();
			for(String f : fields){
				context.write(new Text(f + "->" + name),new Databean(f,1,1));
			}
		}
		
	}
	public static class IndexCombiner extends Reducer<Text, Databean, Text, Databean>{

		private Text k = new Text();
		private Databean cb =  new Databean();
		@Override
		protected void reduce(Text key, Iterable<Databean> ConbinerBeans,
				Reducer<Text, Databean, Text, Databean>.Context context)
				throws IOException, InterruptedException {
			String[] fields = key.toString().split("->");
			long sum = 0;
			for(Databean conbinerBean : ConbinerBeans){
				sum +=conbinerBean.getUpPayLoad();			
			}			
			k.set(fields[0]);
			cb.setUpPayLoad(sum);
			cb.setTelNo(fields[1]);
			context.write(k, cb);
		}		
	}
	public static class IndexReducer extends Reducer<Text, Databean, Text, Resultbean>{

		private Resultbean v = new Resultbean();		
		@Override
		protected void reduce(Text key, Iterable<Databean> values,
				Reducer<Text, Databean, Text, Resultbean>.Context context)
				throws IOException, InterruptedException {
			Map<String, Long> map = new HashMap<String, Long>();
			v.setName(key.toString());
			for(Databean cb : values){
				map.put(cb.getTelNo(), cb.getUpPayLoad());				
			}
			v.setMap(map);
			v.setMapString(v.toString());
			context.write(key, v);
		}
		
	}
}
