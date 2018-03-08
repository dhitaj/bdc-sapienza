import java.io.IOException;
import java.util.*;     
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
        
public class NodeCount {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    	private Text node = new Text();
    	private Text _predicate = new Text();
    	private Text _subject = new Text();
    	private Text _object = new Text();

        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

	  if (!line.trim().isEmpty() && !line.startsWith("#")){


		int first_space = line.indexOf(" ");
		String subject = line.substring(0, first_space);
		String withoutSubject = line.substring(first_space+1, line.length());
		int second_space = withoutSubject.indexOf(" ")+first_space+1;
		String predicate = line.substring(first_space+1, second_space);

		int third_space=0;
		int after_obj=0;
		int end_object=0;

		String check_delimiter = line.substring(second_space+1,second_space+2);
		if (check_delimiter.equals("<")){
				String withoutSubPred = line.substring(second_space+1, line.length());
				end_object = withoutSubPred.indexOf(">")+second_space+1 +1;	 
		}else if(check_delimiter.equals("_")){
			String withoutSubPred = line.substring(second_space+1, line.length());
			end_object = withoutSubPred.indexOf(" ")+second_space+1;		
		}else if(check_delimiter.equals("\"")){
			
			//here I need to check if there is a @ after the " or if there is a ^^ after the " but not after \"
			String withoutSubPred = line.substring(second_space+2, line.length());
			boolean flag=false;
			int future_quote= withoutSubPred.indexOf("\"")+second_space+2;
			int i=0;
			while (flag==false){		
				if(!line.substring(future_quote-1,future_quote).equals("\\") || (line.substring(future_quote-1,future_quote).equals("\\") && line.substring(future_quote-2,future_quote-1).equals("\\"))){
								
					//here is where we check after the " since it is not escaped
					if(line.substring(future_quote+1,future_quote+2).equals("@")){
						String afterclosingquote = line.substring(future_quote+1, line.length());
						end_object=afterclosingquote.indexOf(" ")+future_quote+1;
						flag=true;
					}else if(line.substring(future_quote+1,future_quote+3).equals("^^")){
						String afterclosingquote = line.substring(future_quote+1, line.length());
						end_object=afterclosingquote.indexOf(">")+future_quote+1+1;
						flag=true;
					}else{
						String afterclosingquote = line.substring(future_quote+1, line.length());
						end_object=afterclosingquote.indexOf(" ")+future_quote+1;
						flag=true;
					}
				} else{
					i++;
					withoutSubPred = line.substring(second_space+1+i, line.length());
					future_quote = withoutSubPred.indexOf("\"")+second_space+2+i; 
				}
		}
	
		}		
							
					
					String object = "";
					if(end_object != 0){
						
					object = line.substring(second_space+1,end_object);
					}
					
					String last = line.substring(end_object+1, line.length());
					int sp_ind4 = last.indexOf(" .")+end_object+1;
					String context_line="";
					context_line = line.substring(end_object+1, sp_ind4);

					node.set(subject);
					context.write(node,one);
				
					node.set(object);
					context.write(node,one);
					
					_subject.set(subject+"_subject");
					context.write(_subject,one);
				
					_object.set(subject+"_object");
					context.write(_object,one);
				
					_predicate.set(predicate+"_predicate");
					context.write(_predicate,one);

    }
 } 
}
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }

public static class Map2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        	private Text node = new Text();
        	private Text _predicate = new Text();
        	private Text _subject = new Text();
        	private Text _object = new Text();

            
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

    	  if (!line.trim().isEmpty() && !line.startsWith("#")){
		
    					if(line.contains("_subject")){
    						String subject = "";
    						String[] parts = line.split("\\s");
    					    int k = parts.length;
    					    String part1 = parts[k-1]; 
    					    subject = "outdegree-"+part1;
    					    _subject.set(subject);
        				    context.write(_subject,one);
    					}else if(line.contains("_object")){
    						String object = "";
    						String[] parts = line.split("\\s");
    					    int k = parts.length;
    					    String part1 = parts[k-1]; 
    					    object = "indegree-"+part1;
    					    _object.set(object);
        					context.write(_object,one);
    					}else if(line.contains("_predicate")){
    						_predicate.set("Number of Edges:");
        					context.write(_predicate,one);
    					}else{
    						node.set("Nodes ");
        					context.write(node,one);
    					}

        }
     } 
    }
    
    
public static class Reduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
          throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
   }



public static class Map3 extends Mapper<LongWritable, Text, IntWritable, Text> {

private IntWritable keyToEmit = new IntWritable();
  	private Text _subject = new Text();
	private Text _teksti = new Text();
  	      
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();

	  if (!line.trim().isEmpty() && !line.startsWith("#")){
			if(line.contains("_subject")){
				String subject = "";
				String[] parts = line.split("\\s");
			    int k = parts.length;
			    String part1 = parts[k-1]; 
			    subject = part1;
			    _subject.set(subject);
			    String[] t = line.split("\\_subject");
			    keyToEmit.set(Integer.parseInt(part1));
			    _teksti.set(t[0]);
			    context.write(keyToEmit,_teksti);
			}
     }
 } 
}

public static class DescendingIntegerComparator extends WritableComparator {

protected DescendingIntegerComparator() {
	super(IntWritable.class, true);
}

@SuppressWarnings("rawtypes")

@Override
public int compare(WritableComparable w1, WritableComparable w2) {
	IntWritable k1 = (IntWritable)w1;
	IntWritable k2 = (IntWritable)w2;
	
	return -1 * k1.compareTo(k2);
}
}
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "nodecount");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
    
    
	Job job2 = new Job(conf, "final_count");
	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(IntWritable.class);

	job2.setMapperClass(Map2.class);
	job2.setReducerClass(Reduce2.class);

	job2.setInputFormatClass(TextInputFormat.class);
	job2.setOutputFormatClass(TextOutputFormat.class);

	FileInputFormat.addInputPath(job2, new Path(args[1]));
	FileOutputFormat.setOutputPath(job2, new Path(args[2]));
         
     job2.waitForCompletion(true);
     
     
     Job job3 = new Job(conf, "max_outdegree");
     
     job3.setOutputKeyClass(IntWritable.class);
     job3.setOutputValueClass(Text.class); 
     job3.setSortComparatorClass(DescendingIntegerComparator.class);   
     job3.setMapperClass(Map3.class);
         
     job3.setInputFormatClass(TextInputFormat.class);
     job3.setOutputFormatClass(TextOutputFormat.class);
         
     FileInputFormat.addInputPath(job3, new Path(args[1]));
     FileOutputFormat.setOutputPath(job3, new Path(args[3]));
         
     job3.waitForCompletion(true);
 }
        
}

