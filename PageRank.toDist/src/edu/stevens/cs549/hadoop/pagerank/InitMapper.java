package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/*
		 * TODO done: Just echo the input, since it is already in adjacency list
		 * format.
		 */
		
		// Input: nodeID: nodeIDLinkedTo
		// Output: key-node value-nodeLinkedTo
		System.out.println("InitMapper map " + String.valueOf(key.get()) + " " + value.toString());
		String[] values = line.split(":");
		String nodeKey = values[0].trim();
		String nodeVal = values[1].trim();
		context.write(new Text(nodeKey), new Text(nodeVal));
	}

}
