package MovieRatingPackage;

import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class RatingsMapper extends
			Mapper<Object, Text, Text, Text>{
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		int j = 0;
		String record = value.toString();
		String[] parts = record.split(",");
		
		if (parts[1].equals("movieId")) {
			j=1;
		}
		if(j != 1) {
			context.write(new Text(parts[1]), new Text("ratings\t" + parts[2]));
		}	
	}
}