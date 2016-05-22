package gap;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SequenceGap{
	 public static class TokenizerMapper1
     extends Mapper<LongWritable, Text, LongWritable, Text>{
	  
	  private static long wIns(long i, long j)
	  {
		  long ret = j*j - i*i;
		  return ret;
	  }
	  
	  private static long wDel(long i, long j)
	  {
		  long ret = j*j*j - i*i*i;
		  return ret;
	  }
	  
  public void map(LongWritable key, Text value, Context context
                  ) throws IOException, InterruptedException {
  	Configuration conf = context.getConfiguration();
  	long m = conf.getLong("M", 0);
		long n = conf.getLong("N", 0);
		 
		// Get i,j,val
		String[] tokens = value.toString().split(" ");
		//System.out.println(value.toString());
		long val = Long.parseLong(tokens[1]);
		long location = Long.parseLong(tokens[0]);
		String str1 = tokens[2];
		String str2 = tokens[3];
		String result = "";
		//System.out.println("Mapper key is " + location);
	      
		long i = location / (n+1);
		long j = location % (n+1);
		//System.out.println("Mapper i is: " + i + " j is: "+j);
		
		if(i > 0 && j == 0)
		{
			for(long k = 1; k <= n; k++)
			{
				result = ""+(val + wDel(0, k))+" "+str1+" "+str2;
				context.write(new LongWritable(i*(n+1)+k),  new Text(result));
			}
		}
		else if(j > 0 && i == 0)
		{
			for(long k = 1; k <= m; k++)
			{
				result = ""+(val + wIns(0, k))+" "+str1+" "+str2;
			//	System.out.println("Mapper result is: " + i + " j is: "+j);
				context.write(new LongWritable(k*(n+1)+j),  new Text(result));
			}
		}/*
		else if(i > 0 &&  j > 0)
		{
			System.out.println("Eror in input");
			return;
		}*/
		
		result = ""+val+" "+str1+" "+str2;
		context.write(new LongWritable(i*(n+1)+j),  new Text(result));
  }
}
 
  public static class TokenizerMapper2
  extends Mapper<LongWritable, Text, LongWritable, Text>{
	  private static long wIns(long i, long j)
	  {
		  long ret = j*j - i*i;
		  return ret;
	  }
	  public void map(LongWritable key, Text input_value, Context context )
				throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		long n = conf.getLong("N", 0);
		long m = conf.getLong("M", 0);
		long t = conf.getLong("T", 0);		
		
		// Get i,j,val
		String[] tokens = input_value.toString().split(" ");
		//System.out.println(input_value.toString());
		
		long val = Long.parseLong(tokens[1]);
		long location = Long.parseLong(tokens[0]);
		String str1 = tokens[2];
		String str2 = tokens[3];
		//System.out.println("Mapper key is " + location);
		
		long i = location / (n+1);
		long j = location % (n+1);
		
		if (j == 0 && i+1 <= m) {
			
			// G[i,1] = min { G[i,1] , G[i-1, 0] + s(xi, y1);
			long emit_key = (i + 1) * (n+1) + 1;
			
			if ( str1.toCharArray()[ (int) i + 1 ] != str2.toCharArray()[1] ) {
				context.write(new LongWritable(emit_key), new Text("" + (val+1) + " " + str1 + " " + str2));
			} else {
				context.write(new LongWritable(emit_key), new Text("" + val + " " + str1 + " " + str2));
			}
		}

		context.write(new LongWritable(location), new Text("" + val + " " + str1 + " " + str2));	
	}
}
  
  public static class TokenizerMapper3
  extends Mapper<LongWritable, Text, LongWritable, Text>{
	  private static long wIns(long i, long j)
	  {
		  long ret = j*j - i*i;
		  return ret;
	  }
	  
	  public void map(LongWritable key, Text input_value, Context context )
				throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		long n = conf.getLong("N", 0);
		long m = conf.getLong("M", 0);
		long t = conf.getLong("T", 0);		
		
		// Get i,j,val
		String[] tokens = input_value.toString().split(" ");
		//System.out.println(input_value.toString());
		
		long val = Long.parseLong(tokens[1]);
		long location = Long.parseLong(tokens[0]);
		String str1 = tokens[2];
		String str2 = tokens[3];
		
		
		long i = location / (n+1);
		long j = location % (n+1);
		
		// Loop 3 
		// for j ← 1 to n do
		//    G[1, j] ← min {G[1, j], G[0, j − 1] + s(x1, yj )} 
		
		if (i == 0 && j+1 <= n) {
			long emit_key = 1 * (n+1) + (j+1);
			
			if ( str1.toCharArray()[1] != str2.toCharArray()[(int) j + 1] ) {
				context.write(new LongWritable(emit_key), new Text("" + (val+1) + " " + str1 + " " + str2));
			}
			else {
				context.write(new LongWritable(emit_key), new Text("" + val + " " + str1 + " " + str2));
			}
			
		}
		
		context.write(new LongWritable(location), new Text("" + val + " " + str1 + " " + str2));
		
	}
}  

  public static class TokenizerMapper4
  extends Mapper<LongWritable, Text, LongWritable, Text>{
	  private static long wIns(long i, long j)
	  {
		  long ret = j*j - i*i;
		  return ret;
	  }
	  
	  public void map(LongWritable key, Text input_value, Context context )
				throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		long n = conf.getLong("N", 0);
		long m = conf.getLong("M", 0);
		long t = conf.getLong("T", 0);		
		
		// Get i,j,val
		String[] tokens = input_value.toString().split(" ");
		//System.out.println(input_value.toString());
		
		long val = Long.parseLong(tokens[1]);
		long location = Long.parseLong(tokens[0]);
		String str1 = tokens[2];
		String str2 = tokens[3];
		
		
		long i = location / (n+1);
		long j = location % (n+1); 
		
		if (Math.max(1, t+1-n) <= i && i <= Math.min(t, m)) {
			
			if ( j == t + 1 - i) {
				
			//	System.out.println("T: " + t + " i: " + i + " j: " + j);
				if (i < m && j < n) {
					
					long valplusone = val + 1; 
					if (str1.toCharArray()[(int) i + 1] != str2.toCharArray()[(int) j + 1] ) {
													// G[i+1, j+1]
						context.write(new LongWritable((i+1) * (n+1) + (j+1)), new Text("" + valplusone + " " + str1 + " " + str2));
					} else {
														// G[i+1, j+1]
						context.write(new LongWritable((i+1) * (n+1) + (j+1)), new Text("" + val + " " + str1 + " " + str2));
					}				
					
				}
				
				for(long q = j+1; q <= n; q++ ){ 
					long cost = val + wIns(j, q);
					context.write(new LongWritable( i*(n+1) + q) ,  new Text("" + cost + " " + str1 + " " + str2));
				}
				
				for(long p = i + 1; p <= m; p++) {
					long cost = val + wIns(i, p);
					context.write(new LongWritable( p*(n+1) + j) ,  new Text("" + cost + " " + str1 + " " + str2));
				}
			}
		}
		// Emit self so that cell comes in output
		String emit_value = "" + val + " " + str1 + " " + str2;
		context.write(new LongWritable(location), new Text(emit_value));
	}
}  
  
  
  public static class IntSumReducer
       extends Reducer<LongWritable, Text, LongWritable, Text> {

    public void reduce(LongWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      long min = Long.MAX_VALUE;
      Configuration conf = context.getConfiguration();
      long m = conf.getLong("M", 0);
      long n = conf.getLong("N", 0);
	  long t = conf.getLong("T", 0);
      String str1 = "";
      String str2 = "";
      long val = 0;
      
      long i = key.get() / (n+1);
	  long j = key.get() % (n+1);
	  //System.out.println("Reducer i is: " + i + " j is: "+j);
      	
      for (Text value : values) {
        String[] tokens = value.toString().split(" ");
  		val = Long.parseLong(tokens[0]);
  		str1 = tokens[1];
  		str2 = tokens[2];  		
  		//System.out.println("Reducer: key: " + key.get() + " val: " + val);
        if(val < min)
        {
        	min = val;
        }
      }
      //System.out.println("Reducer: key: " + key.get() + " val: " + min);
	  context.write(key,  new Text("" + min + " " + str1 + " " + str2));
    }
  }

  private static Job getJob(String[] args, Configuration conf, int jobNumber) throws IOException {
	    Job job = new Job();
	  
	  	switch(jobNumber)
	  	{
	  		case 0:
	  			//System.out.println("PART_0 STARTING ---------------------");
			    job = Job.getInstance(conf, "Sequence Alignment part1 ");
			    job.setMapperClass(TokenizerMapper1.class);
			    break;
	  		case 1:
	  			//System.out.println("PART_1 STARTING ---------------------");
			    job = Job.getInstance(conf, "Sequence Alignment part2 ");
			    job.setMapperClass(TokenizerMapper2.class);
			    break;
	  		case 2:
	  			//System.out.println("PART_2 STARTING ---------------------");
			    job = Job.getInstance(conf, "Sequence Alignment part3 ");
			    job.setMapperClass(TokenizerMapper3.class);
			    break;
	  		case 3:
	  			//System.out.println("PART_3 STARTING ---------------------");
			    job = Job.getInstance(conf, "Sequence Alignment part4 ");
			    job.setMapperClass(TokenizerMapper4.class);
			    break;
	  		default:
	  			System.out.println("Wrong Job number");
	  			break;
	  	}
	  	job.setJarByClass(SequenceGap.class);
	  	job.setReducerClass(IntSumReducer.class);
	  	job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		return job;
	}
  
	private static boolean keepGoing(int iterationCount) {
	/* four internal for loops */
		if(iterationCount >= 4) {
	      return false;
	    }
	    return true;
	}
  
  public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	
	String input = args[0];
    String output = args[1];
    String final_output = args[1];
    
    long m = Long.parseLong(args[2]);
    long n = Long.parseLong(args[3]);
    

   // String pathname1 = "input1.txt";
	//String pathname2 = "input2.txt";
	
	//InputHelper.runHelper(m, n, pathname1, pathname2);
	
    conf.setLong("M", m);
	conf.setLong("N", n);
    
    conf.set("mapreduce.output.textoutputformat.separator", " ");
    
	long iterationCount = 0;
    while (iterationCount <= 2) { // Run loop1, loop2, loop3   	
    	
    	//conf.addResource(new Path("/home/parag/hadoop-2.7.2/etc/hadoop/core-site.xml"));
        //conf.addResource(new Path("/home/parag/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"));
        
    	Job job = getJob(args, conf, (int) iterationCount);
    	
	    if (iterationCount > 0)
	    	input = output;
	    
	    output = "hdfs://localhost:9000/tmp/output-" + (iterationCount); 
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.waitForCompletion(true);
	    iterationCount++;
	}
    
    for(long t = 1; t <= (m+n-1); t++) {
		//System.out.println("Filling T : " + t );
    	conf.setLong("T", t);
    	Job job = getJob(args, conf, 3);
	    
    	input = output;
	    
	    if (t < m + n - 1) {
	    	output = "hdfs://localhost:9000/tmp/output-t" + t; 
	    } else {
	    	output = final_output;
	    }
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    job.waitForCompletion(true);
    }
  }
}
