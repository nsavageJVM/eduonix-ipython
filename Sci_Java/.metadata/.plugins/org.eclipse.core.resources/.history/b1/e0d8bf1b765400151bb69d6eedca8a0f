package simple.scijava;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimpleMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	    String line = value.toString();
	    
	    if (!(line.length() == 0)) {
            
            //date
            
            String date = line.substring(6, 14);

            //maximum temperature
            
            float temp_Max = Float
                    .parseFloat(line.substring(39, 45).trim());
            
            //minimum temperature
            
            float temp_Min = Float
                    .parseFloat(line.substring(47, 53).trim());

        //if maximum temperature is greater than 35 , its a hot day
            
            if (temp_Max > 35.0) {
                // Hot day
                context.write(new Text("Hot Day " + date),
                        new Text(String.valueOf(temp_Max)));
            }

            //if minimum temperature is less than 10 , its a cold day
            
            if (temp_Min < 10) {
                // Cold day
                context.write(new Text("Cold Day " + date),
                        new Text(String.valueOf(temp_Min)));
            }
        }


  }
}
