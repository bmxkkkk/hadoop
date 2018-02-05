package com.dick.hadoop.mr;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.dick.hadoop.beans.Databean;

public class DataCount {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf =  new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(DataCount.class);
		job.setMapperClass(DCMapper.class);
		job.setReducerClass(DCReducer.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Databean.class);
		job.setPartitionerClass(ProviderPartitioner.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		job.waitForCompletion(true);
	}
	public static class DCMapper extends Mapper<LongWritable, Text, Text, Databean>{

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Databean>.Context context)
				throws IOException, InterruptedException {
			String[] ss =  value.toString().split("\t");
			Databean databean  =  new Databean(String.valueOf(ss[1]),Long.parseLong(String.valueOf(ss[8])),Long.parseLong(String.valueOf(ss[9])));
			context.write(new Text(String.valueOf(ss[1])), databean);	
			
		}
		
	}
	public static class DCReducer extends Reducer<Text, Databean, Text, Databean>{

		@Override
		protected void reduce(Text key, Iterable<Databean> databeans, Reducer<Text, Databean, Text, Databean>.Context context)
				throws IOException, InterruptedException {
				long upSum = 0L;
				long downSum = 0L;
				for (Databean databean : databeans) {
					upSum+=databean.getUpPayLoad();
					downSum+=databean.getDownPayLoad();
				}
				Databean db =  new Databean(key.toString(),upSum,downSum);
				context.write(key, db);

		}
				
	}
	public static class ProviderPartitioner extends Partitioner<Text, Databean>{
		private static Map<String,Integer> providerMap = new HashMap<String,Integer>();
		static {
			providerMap.put("135", 1);
			providerMap.put("136", 1);
			providerMap.put("137", 1);
			providerMap.put("138", 1);
			providerMap.put("139", 1);
			providerMap.put("150", 2);
			providerMap.put("159", 2);
			providerMap.put("182", 3);
			providerMap.put("183", 3);
			
		}
		@Override
		public int getPartition(Text key, Databean value, int numPartitions) {
			String account=key.toString();
			String sub_acc  = account.substring(0, 3);
			Integer code = providerMap.get(sub_acc);
			if(code==null) code = 0;			
			return code;
		}
		
	}
}
