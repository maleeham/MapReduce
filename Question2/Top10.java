import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
//using job chaining method
public class Top10 {
	// Record Reader reads input from input file line by line and converts it to key,value pairs for mapper1, it is basically same as mutual friend code
    // Method will form pairs of friends such that smaller id is on the left side
    // INPUT:  (byteoffset,<entire line>) from Record Reader
    // OUTPUT : ((friend1,friend2),([friendIDS])) --> friend1.friend2 -> key &[friendIDS] ->value
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

		private Text pair = new Text(); //to store user Pairs

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] input = value.toString().split("\t"); // as word separated by tab spaces
			if (input.length == 2) {
				String userId = input[0]; //input validation if not two then continue //user id
				List<String> friendIds = Arrays.asList(input[1].split(","));//friend ids
				for (String friendId : friendIds) {
					if (Integer.parseInt(userId) > Integer.parseInt(friendId)) //Create pair in sorted order
	                    // (friend1, friend2) is same as (friend2, friend1)
						pair.set(friendId + "," + userId);
						
					else
						pair.set(userId + "," + friendId);
					context.write(pair, new Text(input[1]));
				}
			}
		}

	}

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		private Text mutualFriends = new Text();
		private Text NoMutualFriend = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> intersectionMap = new HashMap<String, Integer>();//for finding intersection of two lists
			int count = 0;
			StringBuilder finalList = new StringBuilder(); //StringBuilder for mutable string object
			for (Text friendIds : values) {
				List<String> friend = Arrays.asList(friendIds.toString().split(","));
				for (String friendId : friend) {
					if (intersectionMap.containsKey(friendId))	//find intersection part
						{
						finalList.append(friendId + ',');
						count=count+1; //including a count variable to store total
						}
					else
						intersectionMap.put(friendId, 1); //if first occurrence

				}
			}
			if (finalList.lastIndexOf(",") > -1) { 
				finalList.deleteCharAt(finalList.lastIndexOf(","));
				//To remove the last ',' inserted while appending text
			}
			mutualFriends.set(new Text(finalList.toString()));
			
			if(mutualFriends != null && mutualFriends.getLength() != 0) //Discard pairs with no mutual friends
				{
					finalList.insert(0,Integer.toString(count) + '\t'); //adding count to the value, separated by \t space
					mutualFriends.set(new Text(finalList.toString()));
					context.write(key, mutualFriends);
				}
			else
			{
				NoMutualFriend.set("0	No Mutual Friends");
				context.write(key, NoMutualFriend);
			}	
		}	
	}

	
	//job2's mapper swap key and value, sort by key (number of mutual friends of each friend).
		public static class Map2 extends Mapper<Text, Text, LongWritable, Text> {

			  private LongWritable frequency = new LongWritable();

			  public void map(Text key, Text value, Context context)
			    throws IOException, InterruptedException {
				String[] input = value.toString().split("\t"); // as word separated by tab spaces
			    int newVal = Integer.parseInt(input[0]);
			    frequency.set(newVal);
			    context.write(frequency, new Text(key+"\t"+input[1]));
			  }
			}
		
		//output the top 10 users with mutual friends
		public static class Reduce2 extends Reducer<LongWritable, Text, Text, Text> {
			private int idx = 0;
			 
			public void reduce(LongWritable key, Iterable<Text> values, Context context)
			      throws IOException, InterruptedException {
				
				for (Text value : values) {
			    	if (idx < 10) {
			    		idx++;
			    		String[] input = value.toString().split("\t"); // as word separated by tab spaces
			    		context.write(new Text(input[0]),new Text(key+"\t"+input[1])); //Interchange the key,value pairs again
			    	}
			    }
			  }
			}
		


	// Driver program
		 public static void main(String []args) throws Exception {
		        Configuration conf = new Configuration();
		        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		        if (otherArgs.length != 3) {
					System.err.println("Usage: Top10 <input> <output1> <output2>");
					System.exit(2);
				}
		        FileSystem fs = FileSystem.get(conf);
		        
				if (fs.exists(new Path(otherArgs[1])))
					fs.delete(new Path(otherArgs[1]), true);
				
				if (fs.exists(new Path(otherArgs[2])))
					fs.delete(new Path(otherArgs[2]), true);
				
		        String inputPath = otherArgs[0];
		        String tempPath = otherArgs[1];
		        String outputPath = otherArgs[2];
		      
		        
		        //First Job
		        {	//create first job
		            conf = new Configuration();
		            @SuppressWarnings("deprecation")
					Job job = new Job(conf, "Job1");

		            job.setJarByClass(Top10.class);
		            job.setMapperClass(Top10.Map1.class);
		            job.setReducerClass(Top10.Reduce1.class);
		            
		            //set job1's mapper output key type
		            job.setMapOutputKeyClass(Text.class);
		            //set job1's mapper output value type
		            job.setMapOutputValueClass(Text.class);
		            
		            // set job1;s output key type
		            job.setOutputKeyClass(Text.class);
		            // set job1's output value type
		            job.setOutputValueClass(Text.class);
		            //set job1's input HDFS path
		            FileInputFormat.addInputPath(job, new Path(inputPath));
		            //job1's output path
		            FileOutputFormat.setOutputPath(job, new Path(tempPath));

		            if(!job.waitForCompletion(true))
		                System.exit(1);
		        }
		        //Second Job
		        {
		            conf = new Configuration();
		            @SuppressWarnings("deprecation")
					Job job2 = new Job(conf, "Job2");

		            job2.setJarByClass(Top10.class);
		            job2.setMapperClass(Top10.Map2.class);
		            job2.setReducerClass(Top10.Reduce2.class);
		            
		            //set job2's mapper output key type
		            job2.setMapOutputKeyClass(LongWritable.class);
		            //set job2's mapper output value type
		            job2.setMapOutputValueClass(Text.class);
		            
		            //set job2's output key type
		            job2.setOutputKeyClass(Text.class);
		            //set job2's output value type
		            job2.setOutputValueClass(Text.class);

		            job2.setInputFormatClass(KeyValueTextInputFormat.class);
		            
		            //hadoop by default sorts the output of map by key in ascending order, set it to decreasing order
		            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		            job2.setNumReduceTasks(1);
		            //job2's input is job1's output
		            FileInputFormat.addInputPath(job2, new Path(tempPath));
		            //set job2's output path
		            FileOutputFormat.setOutputPath(job2, new Path(outputPath));

		            System.exit(job2.waitForCompletion(true) ? 0 : 1);
		        }
		    }

}

