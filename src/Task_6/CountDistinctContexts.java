import java.io.IOException;
import java.util.*;
import java.util.ArrayList;
import java.util.List;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class CountDistinctContexts {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private final static Text contexts = new Text();


    	private Text triple = new Text();
 	

        
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

	triple.set(subject+" "+predicate+" "+object);
	contexts.set(context_line);
	context.write(triple,contexts);

	  }
    }
 } 
        
 public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
      ArrayList<String> valuesList = new ArrayList<String>();
	IntWritable final_con = new IntWritable();
        Iterator<Text> ite = values.iterator();
        while(ite.hasNext()) {
            Text t = ite.next();
           if (valuesList.contains(t)==false){
           	 valuesList.add(t.toString());
		}
        }
      final_con.set(valuesList.size());
	    context.write(key,final_con);
            }
 }

public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {

	private IntWritable keyToEmit = new IntWritable();
  	private Text _subject = new Text();
	private Text _teksti = new Text();
  	      
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();

	  if (!line.trim().isEmpty() && !line.startsWith("#")){
			
			   String triple = "";
			   String[] parts = line.split("\\s");
			    int k = parts.length;
			    String part1 = parts[k-1]; 
			    triple = join(parts, " ", 0, k-1);
			    
			    keyToEmit.set(Integer.parseInt(part1));
			    _teksti.set(triple);
			    context.write(keyToEmit,_teksti);
			
           }
    } 

public static String join(Object[] array, String separator, int startIndex, int endIndex) {
      if (array == null) {
          return null;
      }
      if (separator == null) {
          separator = "";
      }
      // endIndex - startIndex > 0:   Len = NofStrings *(len(firstString) + len(separator))
      //           (Assuming that all Strings are roughly equally long)
      int bufSize = (endIndex - startIndex);
      if (bufSize <= 0) {
          return "";
      }

      bufSize *= ((array[startIndex] == null ? 16 : array[startIndex].toString().length())
                      + separator.length());

      StringBuffer buf = new StringBuffer(bufSize);

      for (int i = startIndex; i < endIndex; i++) {
          if (i > startIndex) {
              buf.append(separator);
          }
          if (array[i] != null) {
              buf.append(array[i]);
          }
      }
      return buf.toString();
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
        
        Job job = new Job(conf, "distinctcontexts");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);

    Job job2 = new Job(conf, "max_distinct_context");
     
     job2.setOutputKeyClass(IntWritable.class);
     job2.setOutputValueClass(Text.class); 
     job2.setSortComparatorClass(DescendingIntegerComparator.class);
   
     job2.setMapperClass(Map2.class);
         
     job2.setInputFormatClass(TextInputFormat.class);
     job2.setOutputFormatClass(TextOutputFormat.class);
         
     FileInputFormat.addInputPath(job2, new Path(args[1]));
     FileOutputFormat.setOutputPath(job2, new Path(args[2]));
         
     job2.waitForCompletion(true);


 }
        
}
