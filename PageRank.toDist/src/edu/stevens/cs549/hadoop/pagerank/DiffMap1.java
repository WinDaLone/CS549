package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DiffMap1 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits each line
		if (sections.length > 2) // checks for incorrect data format
		{
			throw new IOException("Incorrect data format");
		}
		/**
		 * TODO done: read node-rank pair and emit: key:node, value:rank
		 */
		// Input format: node+rank nodesLinkedTo
		// Output format: key-node value-rank
		String[] nodeKeys = sections[0].trim().split("\\+");
		String node = nodeKeys[0].trim();
		String rank = nodeKeys[1].trim();
		context.write(new Text(node), new Text(rank));
	}

}
