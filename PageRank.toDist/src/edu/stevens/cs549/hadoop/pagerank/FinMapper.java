package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/*
		 * TODO done output key:-rank, value: node See IterMapper for hints on
		 * parsing the output of IterReducer.
		 */
		// Input: node+rank nodeLinkedTo
		// Output: key-rank value-node
		String[] sections = line.split("\t");
		if (sections.length > 2) {
			throw new IOException("Incorrect data format");
		}
		String[] ranks = sections[0].split("\\+");
		double rank = Double.parseDouble(ranks[1].trim());

		String node = ranks[0].trim();
		context.write(new DoubleWritable(rank), new Text(node));
	}

}
