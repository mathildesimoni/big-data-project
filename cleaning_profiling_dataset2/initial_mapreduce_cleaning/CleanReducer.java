import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// write the column names for the dataset
		String[] columnArr = {"Total health care", "Total private", "Primary private", "Duplicate private", "Complementary private", "Supplementary private"};
		
		// add values to columnArr if they exist
		for (Text value : values) {
			for (int i = 0; i < columnArr.length; i++) {
				if (value.toString().contains(columnArr[i])) {
					columnArr[i] += ":";
					columnArr[i] += value.toString().split(":")[1]; 
				}
			}
		}

		// write the value as a string for the dataset
		String valueReduce = "";
		for (String column : columnArr) {
			valueReduce += ",";
			if (column.contains(":")){
				valueReduce += column.split(":")[1];
			} else {
				valueReduce += "NONE";
			}
		}
		context.write(key, new Text(valueReduce));
	}
}



		