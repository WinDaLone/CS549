package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] ranks = new double[2];
		/*
		 * TODO done: The list of values should contain two ranks. Compute and
		 * output their difference.
		 */
		// Input format: Key-Node Value-rank
		// Output format: node rankDiff
		Iterator<Text> iterator = values.iterator();
		int count = 0;
		while (iterator.hasNext() && count < 2) {
			ranks[count] = Double.valueOf(iterator.next().toString());
			count++;
		}
		double diff = Math.abs(ranks[0] - ranks[1]);
		String diffString = String.valueOf(diff);
		context.write(key, new Text(diffString));
	}
}
