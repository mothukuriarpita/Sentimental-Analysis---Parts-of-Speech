package Assignment1b.POS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

public class POS extends Configured implements Tool{
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private static HashMap<String, String> posMap = new HashMap<String, String>();
		public void setup(Context context) throws IOException {
			Path p = new Path("hdfs://cshadoop1/user/axm163631/assignment1/pos_words.txt");
            posMap = load(p);
		}

        private HashMap<String, String> load(Path path) throws IOException {
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            HashMap<String, String> words = new HashMap<String, String>();
            while ((line = br.readLine()) != null) {
                String[] w = line.split("×");
                if(w.length < 2) {
                    continue;
                }
                words.put(w[0],w[1]);
            }
            br.close();
            return words;
        }
 
       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	   String temp1=value.toString();
    	   StringTokenizer itr = new StringTokenizer(temp1," ");
    	   while (itr.hasMoreTokens()) {
    		   String tempword=itr.nextToken();
    		   tempword = tempword.replaceAll("[^a-zA-Z]+", "").toLowerCase();
    		   if(tempword.length()<5 || !posMap.containsKey(tempword))
    		   {
    			   continue;
    		   }
    		   String cat=posMap.get(tempword);
    		   if(cat == null || cat.length() ==0) {
    			   continue;
    		   }
    		   context.write(new Text(Integer.valueOf(tempword.length()).toString()),new Text(cat));
    	   }
       }
	}

    public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
    	@Override
    	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
    		long adj=0;
    		long noun=0;
    		long verb=0;
    		long adv=0;
    		long conj=0;
    		long prep=0;
    		long interjection=0;
    		long pronoun=0;
    		long article=0;
    		long nominative=0;
    		long count=0;
    		Iterator<Text> itr=values.iterator();
    		while(itr.hasNext())
    		{
    			String pos=itr.next().toString();
    			if(pos.contains("N") || pos.contains("p") || pos.contains("h"))
    			{
    				noun++;
    			}
    			if(pos.contains("V") || pos.contains("t") || pos.contains("i"))
    			{
    				verb++;
    			}
    			if(pos.contains("A"))
    			{
    				adj++;
    			}
    			if(pos.contains("v"))
    			{
    				adv++;
    			}
    			if(pos.contains("C"))
    			{
    				conj++;
    			}
    			if(pos.contains("P"))
    			{
    				prep++;
    			}
    			if(pos.contains("!"))
    			{
    				interjection++;
    			}
    			if(pos.contains("r"))
    			{
    				pronoun++;
    			}
    			if(pos.contains("D") || pos.contains("I"))
    			{
    				article++;
    			}
    			if(pos.contains("o"))
    			{
    				nominative++;
    			}
    			count++;
    		}
    		context.write(key, new Text("\nLength : "+key +" \nCount of Words: "+ count + "\nDistribution of POS: { " + "NOUN : "+ noun + "; PRONOUN "+
           pronoun + "; VERB "+ verb + " ;ADVERB "+ adv + "; ADJECTIVE : "+ adj + "; CONJUNCTION :"+conj+"; PREPOSITION: "+prep+"; INTERJECTION :"+ interjection +"; NOMINATIVE :"+ nominative +"}"));
    	}
    }
   
	public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new POS(), args);
	    System.exit(res);
	  }

	public int run(String[] args) throws Exception {
		  	Configuration conf = new Configuration();
	        conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
	        conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
	        conf.set("mapreduce.framework.name", "yarn");
	        Job job = Job.getInstance(getConf(), "pos count");
	        job.setJarByClass(this.getClass());
	        // Use TextInputFormat, the default unless job.setInputFormatClass is used
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        job.setMapperClass(Map.class);
	        job.setReducerClass(IntSumReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        return job.waitForCompletion(true) ? 0 : 1;
	  }
}
