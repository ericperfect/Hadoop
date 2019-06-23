package cn.itcast.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCRunner {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(WCRunner.class);
		
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("/wc/srcdata"));
		FileOutputFormat.setOutputPath(job, new Path("/wc/output"));
		
		job.waitForCompletion(true);
	}
	

}
