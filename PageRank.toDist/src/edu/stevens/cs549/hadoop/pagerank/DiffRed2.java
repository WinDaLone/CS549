package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DiffRed2 extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double diff_max = 0.0; // sets diff_max to a default value
		/*
		 * TODO done: Compute and emit the maximum of the differences
		 */
		// Input: key-"Difference" value-rankDiff
		// Output: maxDiff
		Iterator<Text> iterator = values.iterator();
		while (iterator.hasNext()) {
			double v = Double.valueOf(iterator.next().toString());
			if (v > diff_max)
				diff_max = v;
		}

		context.write(new Text(String.valueOf(diff_max)), null);

	}
}
