package MovieRatingPackage;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MovieJoinReducer  extends
			Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
			String movieName = "";
			double total = 0.0;
			int count = 0;
			
		//	System.out.println("Text key   >>" +key.toString());
			for (Text t: values) {
				String parts[] = t.toString().split("\t");
			//	System.out.println("Text values >" +t.toString());
				
				if (parts[0].equals("movies")) {
					movieName = parts[1];
				}else if (parts[0].equals("ratings")) {
					count ++;
					String movieRating = parts[1].trim();
					total += Double.parseDouble(movieRating);
				}
			}
			
			double avgRating = total / count;    // average rating of the movie
			String str = String.format("%d\t%f", count, avgRating);
			context.write(new Text(movieName), new Text(str));
		}
}
