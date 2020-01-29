import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import static java.util.Calendar.*;
import java.util.Date;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class AverageAge {
	public static List<String> tempBuffer = new ArrayList<String>();

	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] inputSplit = value.toString().split("\t");
			if (inputSplit.length == 2) {
				String user = inputSplit[0];
				List<String> friendList = Arrays.asList(inputSplit[1].split(","));
				for (String friend : friendList) {
					context.write(new Text(user), new Text(friend));
				}
			}
		}
	}

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

		HashMap<String, String> storeAge = new HashMap<String, String>();
		// store userID,age in this HashMap in memory
		HashMap<String, Double> map = new HashMap<String, Double>();
		ValueComparator desc = new ValueComparator(map);
		TreeMap<String, Double> sorted_map = new TreeMap<String, Double>(desc);

		
		// override the default in memory join
		protected void setup(Context context) throws IOException, InterruptedException {
			try {
				URI[] paths = context.getCacheFiles();
				Path pt = new Path(paths[0].getPath()); // Location of file in HDFS
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null) {

					String[] arr = line.split(",");
					if (arr.length >= 2) { // put (userID, (age) in the HashMap variable
						storeAge.put(arr[0], arr[9]); // put UserID, age in the map
					}
					line = br.readLine();
				}
			} catch (Exception e) {
			};
		}

		public static int getDiffYears(Date first, Date last) {
			Calendar a = getCalendar(first);
			Calendar b = getCalendar(last);
			int diff = b.get(YEAR) - a.get(YEAR);
			if (a.get(MONTH) > b.get(MONTH) || (a.get(MONTH) == b.get(MONTH) && a.get(DATE) > b.get(DATE))) {
				diff--;
			}
			return diff;
		}

		public static Calendar getCalendar(Date date) {
			Calendar cal = Calendar.getInstance(Locale.US);
			cal.setTime(date);
			return cal;
		}

		
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			// System.out.println(key+" List "+values);
			for (Text friend : values) {
				if (storeAge.containsKey(friend.toString())) {
					Date todayDate = new Date();
					String dob = storeAge.get(friend.toString());
					Date date1 = null;
					try {
						date1 = new SimpleDateFormat("MM/dd/yyyy").parse(dob);
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					int userAge = getDiffYears(date1, todayDate);

					sum = sum + userAge;
					count = count + 1;
				}
			}
			Double avgAge = new Double(sum / count);
			map.put(key.toString(), avgAge);
			//context.write(key, new Text(String.valueOf(avgAge)));
		}

		// Defining ValueComparator explicitly so that we can sort the map in
		// Descending Order
		class ValueComparator implements Comparator<String> {

			Map<String, Double> base;

			public ValueComparator(Map<String, Double> base) {
				this.base = base;
			}

			public int compare(String a, String b) {
				Double valueA = (Double) base.get(a);
				Double valueB = (Double) base.get(b);
				if (valueA >= valueB) {
					return -1;
				} else {
					return 1;
				}
			}
		}


		protected void cleanup(Context context) 
				throws IOException, InterruptedException {
			
			sorted_map.putAll(map);
			
			int count = 0;
			
			for (Entry<String, Double> entry : sorted_map.entrySet()) 
			{
				if(count == 15) 
				{ 
					tempBuffer.remove(0);
					count--;
				}
				tempBuffer.add(entry.getKey().toString()+":"+entry.getValue().toString());
				count++;
				//System.out.println("Added - "+tempBuffer);
				//System.out.println(count);
			}
			System.out.println("TempBUFFER - "+tempBuffer);
			for(String S: tempBuffer)
			{	
				String[] value = S.split(":");
				context.write(new Text(value[0].trim()), new Text(value[1].trim()));
				//tempBuffer.remove(0);
				tempBuffer.set(tempBuffer.indexOf(S), value[0].trim());
				//tempBuffer.add(value[0].trim());
				System.out.println("Reduce 1 -"+value[0]+" - "+value[1]+" - "+ tempBuffer.contains(value[0]));
				
			}

		}

	}

	public static class Map2_1 extends Mapper<LongWritable, Text, Text, Text> {

		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] line = value.toString().split("\t");
			//System.out.println("Map 2 1 -"+line[0]+" - "+tempBuffer.contains(line[0].toString()));
			
			if ((line.length == 2) && (tempBuffer.contains(line[0]))) 
			{
				context.write(new Text(line[0]), new Text("age"+"-" +line[1]));
				//System.out.println("Map21"+line[0]+" - "+line[1]);
			}
		}
	}

	public static class Map2_2 extends Mapper<LongWritable, Text, Text, Text> {
		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] line = value.toString().split(",");
			//System.out.println("Map 2 2 -"+line[0]+" - "+line[5]);
			if ((line.length >= 2) && (tempBuffer.contains(line[0].trim()))) 
			{
				context.write(new Text(line[0]), new Text("address" +"-"+ line[3] + "," + line[4] + "," + line[5])); 
				//System.out.println("Map22"+line[0]+" - "+line[5]);
			}
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text> {

		//HashMap<String, String> storeAddress = new HashMap<String, String>();
		//ArrayList<String> ageDetail = new ArrayList<String>();
		//ArrayList<String> addressDetails = new ArrayList<String>();

		protected void reduce(Text key, Iterable<Text> values, Context context)	throws IOException, InterruptedException {
			
			String addressDetails = null;
			String avgAge = null;
			int count = 0;
			int bcount = 1;
			int rcount = 1;
			for (Text t : values) {
				String[] details = t.toString().split("-");
				if (details[0].trim().equals("address") && bcount == 1) {
					addressDetails = details[1];
					count++;
					bcount++;
				} else if (details[0].trim().equals("age") && rcount == 1) {
					avgAge = details[1];
					count++;
					rcount++;
				}
			}
			if (count == 2) {
				System.out.println(key.toString()+"---"+addressDetails+"---"+avgAge);
				context.write(key, new Text(addressDetails + "," + avgAge));
			}
			
			/*
			 * for (Text text : values) { String[] value = text.toString().split("|");
			 * 
			 * if (value[0].trim().equalsIgnoreCase("age")) { ageDetail.add(key.toString() +
			 * ":" + value[1]); } else { addressDetails.add(key.toString() + ":" +
			 * value[1]); } }
			 */			// System.out.println(Arrays.asList(value));
		}
	}

	/*
	 * protected void cleanup(Context context) throws IOException,
	 * InterruptedException { for (String age : ageDetail) { for (String address :
	 * addressDetails) { String[] Id1 = age.split(":"); String userId1 = Id1[0];
	 * String avgAge = Id1[1];
	 * 
	 * String[] Id2 = address.split(":"); String userId2 = Id2[0]; String
	 * fullAddress = Id2[1];
	 * 
	 * if (userId1.equals(userId2)) { //
	 * finalMap.put(userId1,fullAddress+","+avgAge); context.write(new
	 * Text(userId1), new Text(fullAddress + "," + avgAge)); break; } } } } 
	 */
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: AverageAge <input> <output1> <input> <output2>");
			System.exit(2);
		}
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(otherArgs[1])))
			fs.delete(new Path(otherArgs[1]), true);
		if (fs.exists(new Path(otherArgs[3]))) 
			fs.delete(new Path(otherArgs[3]), true);

		String inputPath = otherArgs[0];
		String tempPath = otherArgs[1];
		String inputPath2 = otherArgs[2];
		String outputPath = otherArgs[3];
		
		// First Job
		// create first job
			conf = new Configuration();

			@SuppressWarnings("deprecation")
			Job job = new Job(conf, "Job1");
			
			job.addCacheFile(new Path(inputPath2).toUri());

			job.setJarByClass(AverageAge.class);
			job.setMapperClass(AverageAge.Map1.class);
			job.setReducerClass(AverageAge.Reduce1.class);

			// set job1's mapper output key type
			job.setMapOutputKeyClass(Text.class);
			// set job1's mapper output value type
			job.setMapOutputValueClass(Text.class);

			// set job1;s output key type
			job.setOutputKeyClass(Text.class);
			// set job1's output value type
			job.setOutputValueClass(Text.class);
			// set job1's input HDFS path
			FileInputFormat.addInputPath(job, new Path(inputPath));
			// job1's output path
			FileOutputFormat.setOutputPath(job, new Path(tempPath));

			boolean mapreduce = job.waitForCompletion(true);
		
		// Second Job
			if (mapreduce){
			Configuration conf1 = new Configuration();

			@SuppressWarnings("deprecation")
			Job job2 = new Job(conf1, "Job2");

			job2.setJarByClass(AverageAge.class);
			// job2.setMapperClass(AverageAge.Map2.class);

			job2.setOutputKeyClass(Text.class);
			// set job2's output key type
			// set job2's output value type
			job2.setOutputValueClass(Text.class);
			
			job2.setInputFormatClass(TextInputFormat.class);
			

			MultipleInputs.addInputPath(job2, new Path(tempPath), TextInputFormat.class, Map2_1.class);
			MultipleInputs.addInputPath(job2, new Path(inputPath2), TextInputFormat.class, Map2_2.class);
			job2.setReducerClass(AverageAge.Reduce2.class);
			// set job2's output path
			FileOutputFormat.setOutputPath(job2, new Path(outputPath));
			//job2.setOutputFormatClass(TextOutputFormat.class);

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}
}
,.
