package simple.scijava;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrainingDataMapper extends Mapper<LongWritable, Text, Text, Text> {

	  @Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {

		    String line = value.toString();
		    
		    String[] array = value.toString().split(" +");
		    
		    if (!(line.length() == 0)) {
	           
	                context.write(new Text(array[0] ), new Text(String.format("%s|%s|%s|%s|%s", array[5], array[6], array[9], array[16], array[19]) ));
	            }
	        }


	  }
	

