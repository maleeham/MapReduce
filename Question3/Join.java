import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class Join {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text pair = new Text(); //to store user Pairs
		HashMap<String,String> map = new HashMap<String,String>(); //memory table to get details
		@Override 
		//override the default in memory join
		public void setup(Context context) throws IOException, InterruptedException {
			try{
				URI[] paths = context.getCacheFiles();
				Path pt = new Path(paths[0].getPath()); // Location of file in HDFS
	            FileSystem fs = FileSystem.get(new Configuration());
	            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	            String line;
	            line=br.readLine();
	            while (line != null){
	            	
	            	String[] arr=line.split(","); 
		        	if (arr.length>=2) {					//put (userID, (name+city) in the HashMap variable
		        	map.put(arr[0], arr[1]+':'+arr[4]); 		//put UserID, Name:City in the map
		        	}
	                line=br.readLine();
	            }
	        }catch(Exception e){
	        };
	        //System.out.println(Arrays.asList(map));
		    }
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] input = value.toString().split("\t"); // as word separated by tab spaces
			StringBuilder outputList = new StringBuilder();
			
			if (input.length == 2) {
				String userId = input[0]; //input validation if not two then continue //user id
				List<String> friendIds = Arrays.asList(input[1].trim().split(","));//friend ids
				
				for(String id: friendIds) 
				{
						if (map.containsKey(id)) 
						{	String temp = id+"-"+map.get(id);
							outputList.append(temp); //mapped in map as id, (Name:city)
							outputList.append(",");
							
						}
				}
				if (outputList.lastIndexOf(",") > -1) { 
					outputList.deleteCharAt(outputList.lastIndexOf(",")); //To remove the last ',' inserted while appending text
				}
				
				for (String friendId : friendIds) {
					if (Integer.parseInt(userId) > Integer.parseInt(friendId)) //Create pair in sorted order
						pair.set(friendId + "," + userId);
						
					else
						pair.set(userId + "," + friendId);
				
					
					context.write(pair, new Text(outputList.toString()));
				}
				
				
				//System.out.println((pair+"-"+outputList));
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
				/*
				 * if (key.toString().equals("0,1")) { System.out.println("This is it:"); }
				 */
				List<String> friends = Arrays.asList(friendIds.toString().split(","));
				for (String friend : friends) {
					String[] getId = friend.split("-"); //split by : to extract userID
					//System.out.println((getId[0]+"-"+getId[1]+"-"+getId[2]));
			
					if (intersectionMap.containsKey(getId[0].trim()))	//find intersection part
						{	
							//String[] details = getId[1].split(":"); //split by : to extract name and city
							String name =  getId[1];
							//name.trim();
							//city.trim();
							finalList.append(name+ ',');
						}
					else
						intersectionMap.put(getId[0].trim(), 1); //if first occurrence

				}
			}
			
			if (finalList.lastIndexOf(",") > -1) { 
				finalList.deleteCharAt(finalList.lastIndexOf(",")); //To remove the last ',' inserted while appending text
			}

			mutualFriends.set(new Text(finalList.toString()));
			NoMutualFriend.set("No Mutual Friends");
			if(mutualFriends != null && mutualFriends.getLength() != 0) //Discard pairs with no mutual friends
				{
					context.write(key, mutualFriends);
				//System.out.println(key+"-"+mutualFriends);
				}
			else
				context.write(key,NoMutualFriend);
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();// get all arguments
		
		if (otherArgs.length != 3) {
			System.err.println("Invalid Usage! Correct Usage: Join <inputfile hdfs path> <input userdata file> <output file hdfs path>");
			System.exit(2);
		}
	    
		FileSystem fs = FileSystem.get(config);
		if (fs.exists(new Path(otherArgs[2])))
			fs.delete(new Path(otherArgs[2]), true);
		
		String inputPath = otherArgs[0];
		String inputPath2 = otherArgs[1];
		String outputPath = otherArgs[2];
		
		@SuppressWarnings("deprecation")
		Job job = new Job(config, "Join");
		job.addCacheFile(new Path(inputPath2).toUri());
		
		job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setJarByClass(Join.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(inputPath));
		// set the HDFS path for the output
		//location of table
		
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

.
