
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class MutualFriend {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text pair = new Text(); //to store user Pairs

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] input = value.toString().split("\t"); // as word separated by tab spaces
			if (input.length == 2) {
				String userId = input[0]; //input validation if not two then continue //user id
				List<String> friendIds = Arrays.asList(input[1].split(","));//friend ids
				for (String friendId : friendIds) {
					if (Integer.parseInt(userId) > Integer.parseInt(friendId)) //Create pair in sorted order
						pair.set(friendId + "," + userId);
						
					else
						pair.set(userId + "," + friendId);
					context.write(pair, new Text(input[1]));
				}
			}
		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text mutualFriends = new Text();
		private Text NoMutualFriend = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> intersectionMap = new HashMap<String, Integer>();//for finding intersection of two lists
			StringBuilder finalList = new StringBuilder(); //StringBuilder for mutable string object
			for (Text friendIds : values) {
				List<String> friend = Arrays.asList(friendIds.toString().split(","));
				for (String friendId : friend) {
					if (intersectionMap.containsKey(friendId))	//find intersection part
						finalList.append(friendId + ',');
					else
						intersectionMap.put(friendId, 1); //if first occurrence

				}
			}
			if (finalList.lastIndexOf(",") > -1) { 
				finalList.deleteCharAt(finalList.lastIndexOf(",")); //To remove the last ',' inserted while appending text
			}

			mutualFriends.set(new Text(finalList.toString()));
			NoMutualFriend.set("No Mutual Friends");
			if(mutualFriends != null && mutualFriends.getLength() != 0) //Discard pairs with no mutual friends
				context.write(key, mutualFriends);
			else
				context.write(key,NoMutualFriend);
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();// get all arguments
		if (otherArgs.length != 2) {
			System.err.println("Invalid Usage! Correct Usage: Mutual Friend <inputfile hdfs path> <output file hdfs path>");
			System.exit(2);
		}
		FileSystem fs = FileSystem.get(config);
		if (fs.exists(new Path(otherArgs[1])))
			fs.delete(new Path(otherArgs[1]), true);

		@SuppressWarnings("deprecation")
		Job job = new Job(config, "MutualFriend");
		job.setJarByClass(MutualFriend.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

