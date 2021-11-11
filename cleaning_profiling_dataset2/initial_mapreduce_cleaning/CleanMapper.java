import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] arrayLine = line.split(",");

		// select data for years 2014 to 2018  only
		String[] years = {"2014", "2015", "2016", "2017", "2018"};

		for (String year : years) {
			if ((arrayLine[6].contains(year))){
				// key
				String countryName = arrayLine[5].substring(1, arrayLine[5].length() - 1);
				String keyMapper = countryName + "," + year;

				// value
				String variable = arrayLine[1].substring(1, arrayLine[1].length() - 1);
				String val = arrayLine[8];
				String valueMapper =  variable + ":" + val;

				// write key-value pair
				context.write(new Text(keyMapper), new Text(valueMapper));
			}
		}
	}
}

