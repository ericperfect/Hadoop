package cn.itcast.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jboss.netty.util.internal.StringUtil;
        
@SuppressWarnings("deprecation")
public class sads {
	
	
	/**
	 * Map: 将输入的文本数据转换为<word-1>的键值对
	 * */
	public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		Text word = new Text();
		final static IntWritable one = new IntWritable(1);
		HashSet<String> stopWordSet = new  HashSet<String>();
		
	
		/**
		 *  map
		 * */
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
			
			String s = null;
			String line = value.toString().toLowerCase();
			line = line.replaceAll("[^a-z]", " ");
			line = line.replace("^\\s*|\\s*$", "");
			line = line.replaceAll("\\s{1,}", " ");
			String[] fields = StringUtils.split(line, " ");
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			String fileName = inputSplit.getPath().getName();
			
			for(String field:fields){
				//k: hello-->a.txt    v:1
				context.write(new Text(field+"-->"+fileName), one);
			}
		}
	}
	
	/**
	 * Reduce: add all word-counts for a key
	 * */
	public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		
		/**
		 * reduce
		 * */
		public void reduce(Text key, Iterable<IntWritable> values, Context context)	
			throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	
	public static class WordCountMap2 extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			
			String[] fields = StringUtils.split(line, "\t");
			String[] wordAndfileName = StringUtils.split(fields[0], "-->");
			String word = wordAndfileName[0];
			String fileName = wordAndfileName[1];
			
			int count = Integer.parseInt(fields[1]);
			
			context.write(new Text(word), new Text(fileName+"-->"+count));
			//<hello,a.txt---3>
			
		}
	}
	
	public static class WordCountReduce2 extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			String result = "";
			for(Text value:values){
				
				result += value + " ";
			}
			
			context.write(key, new Text(result));
	}
	}
	/**
	 * main: run two job
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 * */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		boolean exit = false;
		String skipfile = null; //stop-file path
		
		Configuration conf = new Configuration();

			/**
			 * run first-round to count
			 * */
			Job job = new Job(conf, "wyz-wordcountjob-1");
			job.setJarByClass(sads.class);
			
			
			
			//set class of output's key-value of MAP
			job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
		    //set mapper and reducer
		    job.setMapperClass(WordCountMap2.class);     
		    job.setReducerClass(WordCountReduce2.class);
		    
		    //set path of input-output
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    
		    Path output = new Path(args[1]);
		    FileSystem fs = FileSystem.get(conf);
		    if(fs.exists(output)){
		    	fs.delete(output,true);
		    }
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true)?0:1);
		    
	} 
	
			
		 
 }





