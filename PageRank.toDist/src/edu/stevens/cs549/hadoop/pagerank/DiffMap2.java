package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DiffMap2 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String s = value.toString(); // Converts Line to a String

		/*
		 * TODO done: emit: key:"Difference" value:difference calculated in
		 * DiffRed1
		 */
		// Input format: node rankDiff
		// Output format: key-"Difference" value-rankDiff
		String[] sections = s.split("\t");
		if (sections.length > 2) {
			throw new IOException("Incorrect data format");
		}
		String diff = sections[1].trim();
		context.write(new Text("Difference"), new Text(diff));

	}

}
