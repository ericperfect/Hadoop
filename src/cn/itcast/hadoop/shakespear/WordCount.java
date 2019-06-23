package cn.itcast.hadoop.shakespear;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
@SuppressWarnings("deprecation")
public class WordCount {
	
	
	/**
	 * Map: 将输入的文本数据转换为<word-1>的键值对
	 * */
	public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		String regex = "[.,\"!--;:?'\\]]"; //remove all punctuation
		Text word = new Text();
		final static IntWritable one = new IntWritable(1);
		HashSet<String> stopWordSet = new HashSet<String>();
		
		/**
		 * 将停词从文件读到hashSet中
		 * */
		private void parseStopWordFile(String path){
			try {
				String word = null;
				BufferedReader reader = new BufferedReader(new FileReader(path));
				while((word = reader.readLine()) != null){
					stopWordSet.add(word);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}	
			
		}
		
		/**
		 * 完成map初始化工作
		 * 主要是读取停词文件
		 * */
		public void setup(Context context) {			
			
			Path[] patternsFiles = new Path[0];
			try {
				patternsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			} catch (IOException e) {
				e.printStackTrace();
			}			
			if(patternsFiles == null){
				System.out.println("have no stopfile\n");
				return;
			}
			
			//read stop-words into HashSet
			for (Path patternsFile : patternsFiles) {
				parseStopWordFile(patternsFile.toString());
			}
		}  
		
		/**
		 *  map
		 * */
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
			
			String s = null;
			String line = value.toString().toLowerCase();
			line = line.replaceAll(regex, " "); //remove all punctuation
			
			//split all words of line
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				s = tokenizer.nextToken();
				if(!stopWordSet.contains(s)){
					word.set(s);
					context.write(word, one);
				}				
			}
		}
	}
	
	/**
	 * Reduce: add all word-counts for a key
	 * */
	public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		int min_num = 0;
		
		/**
		 * minimum showing words
		 * */
		public void setup(Context context) {
			min_num = Integer.parseInt(context.getConfiguration().get("min_num"));
			System.out.println(min_num);
		}
		
		/**
		 * reduce
		 * */
		public void reduce(Text key, Iterable<IntWritable> values, Context context)	
			throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			if(sum < min_num) return;
			context.write(key, new IntWritable(sum));
		}
	}
	
	/**
	 * IntWritable comparator
	 * */
	private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        
	      public int compare(WritableComparable a, WritableComparable b) {
	    	  return -super.compare(a, b);
	      }
	      
	      public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	          return -super.compare(b1, s1, l1, b2, s2, l2);
	      }
	}
	
	/**
	 * main: run two job
	 * */
	public static void main(String[] args){
		
		boolean exit = false;
		String skipfile = null; //stop-file path
		int min_num = 0;
		String tempDir = "wordcount-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE));
		
		Configuration conf = new Configuration();
		
		//获取停词文件的路径，并放到DistributedCache中
	    for(int i=0;i<args.length;i++)
	    {
			if("-skip".equals(args[i]))
			{
				DistributedCache.addCacheFile(new Path(args[++i]).toUri(), conf);
				System.out.println(args[i]);
			}			
		}
	    
	    //获取要展示的最小词频
	    for(int i=0;i<args.length;i++)
	    {
			if("-greater".equals(args[i])){
				min_num = Integer.parseInt(args[++i]);
				System.out.println(args[i]);
			}			
		}
	    
		//将最小词频值放到Configuration中共享
		conf.set("min_num", String.valueOf(min_num));	//set global parameter
		
		try{
			/**
			 * run first-round to count
			 * */
			Job job = new Job(conf, "wyz-wordcountjob-1");
			job.setJarByClass(WordCount.class);
			
			//set format of input-output
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
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
				Job job2 = new Job(conf, "wyz-wordcountjob-2");
				job2.setJarByClass(WordCount.class);
				
				//set format of input-output
				job2.setInputFormatClass(SequenceFileInputFormat.class);
				job2.setOutputFormatClass(TextOutputFormat.class);		
				
				//set class of output's key-value
				job2.setOutputKeyClass(IntWritable.class);
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
			    job2.setMapperClass(InverseMapper.class);    
			    job2.setNumReduceTasks(1); //only one reducer
			    
			    //set path of input-output
			    FileInputFormat.addInputPath(job2, new Path(tempDir));
			    Path output = new Path(args[1]);
			    FileSystem fs = FileSystem.get(conf);
			    if(fs.exists(output)){
			    	fs.delete(output,true);
			    }
			    	
			    		
			    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
			    
			    /**
			     * Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。
			     * 因此我们实现了一个 IntWritableDecreasingComparator 类,　
				 * 并指定使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行排序
			     * */
			    job2.setSortComparatorClass(IntWritableDecreasingComparator.class);
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
