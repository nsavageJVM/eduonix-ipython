package simple.scijava;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SciDataSimpleTest extends Configured implements Tool {
	
  private static final boolean runOnHDFS = true;	

  private static final String projectRootPath = System.getProperty("user.dir");
  
  private Path output;
  private Path input;
  
 
  public static void main(String[] args) throws Exception {
	  
	  
	  int res = ToolRunner.run(new Configuration(), new SciDataSimpleTest(), args);
      System.exit(res);  
	  
  }

  @Override
  public int run(String[] args) throws Exception {
	    
	   // When implementing tool
    Configuration conf = this.getConf();
    
    Job job = Job.getInstance(conf, "SciData Job");
    
    setTextoutputformatSeparator(job, "|");

    job.setJarByClass(SciDataSimpleTest.class);
    
    //Key type coming out of mapper
    job.setMapOutputKeyClass(Text.class);
    
    //value type coming out of mapper
    job.setMapOutputValueClass(Text.class);

    //Defining the mapper class name  
    // job.setMapperClass(SimpleMapper.class);
    job.setMapperClass(TrainingDataMapper.class);
    //Defining the reducer class name
    job.setReducerClass(SimpleReducer.class);
    
    //Defining input Format class which is responsible to parse the dataset into a key value pair
    job.setInputFormatClass(TextInputFormat.class);
    
    
    //Defining output Format class which is responsible to parse the dataset into a key value pair
    job.setOutputFormatClass(TextOutputFormat.class);

  if(runOnHDFS) {
	    output = new Path("sci_data_1_out");
	    input =  new Path("sci_data_1.txt");
  } else {
	    output = new Path(String.format("%s%s", projectRootPath,"/sci_data_1_out"));
	    input =  new Path(String.format("%s%s", projectRootPath,"/sci_data_1.txt"));
  }

    
    output.getFileSystem(conf).delete(output, true);
       
    FileInputFormat.addInputPath(job, input);
    
    FileOutputFormat.setOutputPath(job, output);
    
    /*
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     */
    job.setJobName("SciData Driver");
    
    

    // Execute job and return status
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  void setTextoutputformatSeparator(final Job job, final String separator){
      final Configuration conf = job.getConfiguration(); //ensure accurate config ref

      conf.set("mapred.textoutputformat.separatorText", separator); // ?
}
}

