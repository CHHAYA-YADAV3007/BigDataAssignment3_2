/*
 * ClassSales1.java 
 * 
 * Compiled on 27th September 2017
 */
//package declaration
package PackageSales;
//importing the packages for MAPREDUCE Program
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.io.IOException;
import java.io.InterruptedIOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
/**
 * THIS CLASS will filter out the records which are having NA value in place of either first parameter company name
 * or second parameter product name in TV SET.
 * @author Chhaya yadav
 * 
 * VERSION 1.1
 * 
 * COMPILED ON 27th Sept 2017
 *
 */

//Class declaration
public class ClassSales1 {

//User Defined SalesMap inheriting the PARENT CLASS Mapper
    
    public static class SalesMap extends Mapper<LongWritable , Text , Text ,NullWritable > {
    
// Defining the abstract method map of Mapper class
        
        public void map(LongWritable key , Text value , Context context)
        throws IOException , InterruptedException
        
        {
                
            
                String line = value.toString();
                
                String[] word = line.split(Pattern.quote("|"));
                    
                IntWritable one = new IntWritable(0);
                
                
                
                if ((word[0].matches("NA")== false )&& (word[1].matches("NA")== false )) {
                    
                    context.write(value, NullWritable.get());
                }
                        
                 
                    
                    
                }
                
                
        }
        
    
        
    public static void main(String[] args) 
    throws Exception  {
        
        
        

        Configuration conf = new Configuration();
        
        Job job = new Job(conf,"sales");
        
        job.setJarByClass(ClassSales1.class);
        
        job.setMapperClass(SalesMap.class);
        
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        
        job.setOutputValueClass(NullWritable.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        
        Path outputPath = new Path(args[1]);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        outputPath.getFileSystem(conf).delete(outputPath);
        
     System.exit(job.waitForCompletion(true)? 0 :1);
     
    }

}