package cn.itcast.hadoop.inverseindex;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
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
        

public class InverseIndex {
	
	
	/**
	 * Map: 将输入的文本数据转换为<word-1>的键值对
	 * */
	public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		
		final static IntWritable one = new IntWritable(1);
		HashSet<String> stopWordSet = new  HashSet<String>();
		
		/**
		 * 完成map初始化工作
		 * 主要是读取停词文件
		 * @throws IOException 
		 * */
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {	
			URI[] cacheFiles = context.getCacheFiles();
			Path cacheFile = new Path(cacheFiles[0]);
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {  
				BufferedReader reader = new BufferedReader(new FileReader("stopword"));
				String word;
				while((word = reader.readLine()) != null){
					stopWordSet.add(word);
				}
				reader.close();
			}
			
            
		}
		
		/**
		 *  map
		 * */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String s = null;
			String line = value.toString().toLowerCase();
			line = line.replaceAll("[^a-z]", " ");
			line = line.replaceAll("^\\s*|\\s*$", "");
			line = line.replaceAll("\\s{1,}", " ");
			
			String[] fields = StringUtils.split(line, " ");
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			String fileName = inputSplit.getPath().getName();
			
			for(String field:fields){
				//k: hello-->a.txt    v:1
				if(!stopWordSet.contains(field)){
					context.write(new Text(field+"-->"+fileName), one);
				}				
				
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
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
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
	 * @throws URISyntaxException 
	 * */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException{
		
		boolean exit = false;
		String tempDir = "wordcount-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE));
		
		Configuration conf = new Configuration();
		
		//获取停词文件的路径，并放到DistributedCache中
	    
	    
	    
		try{
			/**
			 * run first-round to count
			 * */
	    	Job job = Job.getInstance(conf);
	    	conf.set("mapreduce.framework.name", "yarn");
	    	
			job.addCacheFile(new URI("/shakespear/stopword/stopword.txt#stopword"));
				
			job.setJarByClass(InverseIndex.class);
			
			//set format of input-output
			
			
			//set class of output's key-value of MAP
			job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    //set mapper and reducer
		    job.setMapperClass(WordCountMap.class);     
		    job.setReducerClass(WordCountReduce.class);
		    
		    //set path of input-output
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(tempDir));
		    
		    
		    
		    if(job.waitForCompletion(true)){		    
			    /**
			     * run two-round to sort
			     * */
			    //Configuration conf2 = new Configuration();
				Job job2 = Job.getInstance(conf);
				job2.setJarByClass(InverseIndex.class);
				
				//set format of input-output
				
				
				//set class of output's key-value
				job2.setOutputKeyClass(Text.class);
			    job2.setOutputValueClass(Text.class);
			    
			    //set mapper and reducer
			    //InverseMapper作用是实现map()之后的数据对的key和value交换
			    //将Reducer的个数限定为1, 最终输出的结果文件就是一个
				/**
				* 注意，这里将reduce的数目设置为1个，有很大的文章。
				* 因为hadoop无法进行键的全局排序，只能做一个reduce内部
				* 的本地排序。 所以我们要想有一个按照键的全局的排序。
				* 最直接的方法就是设置reduce只有一个。
				*/
			    job2.setMapperClass(WordCountMap2.class);
			    job2.setReducerClass(WordCountReduce2.class);
			    
			    //set path of input-output
			    FileInputFormat.addInputPath(job2, new Path(tempDir));
			    Path output = new Path(args[1]);
			    FileSystem fs = FileSystem.get(conf);
			    if(fs.exists(output)){
			    	fs.delete(output,true);
			    }
			    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
			    
			    
			    exit = job2.waitForCompletion(true);
		    }
		}catch(Exception e){
			e.printStackTrace();
		}finally{
		    
		    try {
		    	//delete tempt dir
				FileSystem.get(conf).deleteOnExit(new Path(tempDir));
				if(exit) System.exit(1);
				System.exit(0);
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
	}
 }