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
        
public class CountBlanks {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);


    	private Text blank_context = new Text("Blank Context");
 	private Text blank_subject = new Text("Blank Subject");
 	private Text blank_object = new Text("Blank Object");
	private Text tot = new Text("Total Lines");


        
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
	//	        	  System.out.println(check);
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
								
							if(!line.substring(future_quote-1,future_quote).equals("\\") || (line.substring(future_quote-1,future_quote).equals("\\")
																					 && line.substring(future_quote-2,future_quote-1).equals("\\"))){
								
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


					if (context_line.equals("")){
				context.write(blank_context,one);

			}
			
			if (object.indexOf("_:")==0){
				context.write(blank_object,one);

			}
			if (subject.indexOf("_:")==0){
				context.write(blank_subject,one);

			}
			
			context.write(tot,one);
						
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
 }
        
}
