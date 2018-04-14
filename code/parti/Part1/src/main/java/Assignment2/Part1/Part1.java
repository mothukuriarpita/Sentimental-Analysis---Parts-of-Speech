package Assignment2.Part1;

import java.util.HashSet;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Part1 {

  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public static HashSet<String> pos = new HashSet<String>();
    public static HashSet<String> neg = new HashSet<String>();
    
    public void setup(Context context) throws IOException{
        
    	Path positiveWordspath = new Path("hdfs://cshadoop1/user/lxp160730/positive-words.txt");
        Path NegativeWordPath = new Path("hdfs://cshadoop1/user/lxp160730/negative-words.txt");
    
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader brpos=new BufferedReader(new InputStreamReader(fs.open(positiveWordspath)));
        BufferedReader brneg=new BufferedReader(new InputStreamReader(fs.open(NegativeWordPath)));
        String line,line1;
  
        while ((line = brpos.readLine()) != null) {
            String NewLine = line.trim().toLowerCase();
            if (NewLine.startsWith(";"))
                continue;
            pos.add(NewLine);
        }
        brpos.close();
        
        while ((line1 = brneg.readLine()) != null) {
            String NewLine1 = line1.trim().toLowerCase();
            if (NewLine1.startsWith(";"))
                continue;
            neg.add(NewLine1);
        }
        brneg.close();
    }
  
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString()," ");
      
      while (itr.hasMoreTokens()) {
    	  String docwords = itr.nextToken().toLowerCase();
    	  if(pos.contains(docwords))
    	  {
        	word.set("positive Words");
    	    context.write(word, one);
    	  }
          else if (neg.contains(docwords))
          {
            word.set("negative Words");
            context.write(word, one);
          }
          else
        	  continue;
         
      }
    }
  }

  public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
	 
	  private IntWritable result = new IntWritable();
	    
	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      
	        for (IntWritable val : values) {
	        sum += val.get();
	        }
	        result.set(sum);
	        context.write(key, result);
	      }
	    
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
    conf.set("mapreduce.framework.name", "yarn");
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Part1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}