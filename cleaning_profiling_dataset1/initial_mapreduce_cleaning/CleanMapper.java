import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper
	extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {

		String line = value.toString();
		String[] fields = line.split(",");
		if (fields.length > 7){
			String year = fields[4];
			String country = fields[0];
			String che_usd = fields[6];
			if (year.equals("2018")) { 
				context.write(new Text(country), new Text(che_usd));
			}
		}
		

		
		
	}
}