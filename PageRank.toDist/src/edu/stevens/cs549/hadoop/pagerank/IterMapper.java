package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits it into two parts. Part
												// 1: node+rank | Part 2: adj
												// list

		if (sections.length > 2) // Checks if the data is in the incorrect
									// format
		{
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;
		}

		/*
		 * TODO done: emit key: adj vertex, value: computed weight.
		 * 
		 * Remember to also emit the input adjacency list for this node! Put a
		 * marker on the string value to indicate it is an adjacency list.
		 */
		// Input: node+rank nodeLinkedTo
		// Output: key-nodeLinkedTo value-rank+length(nodeLinks)
		String[] nodeParts = sections[0].split("\\+");
		String node = nodeParts[0].trim();
		String rank = nodeParts[1].trim();
		// Send to adj node
		if (!sections[1].equals("")) {
			String[] adjLists = sections[1].split(" ");
			String length = String.valueOf(adjLists.length);
			for (String adj : adjLists) {
				String data = rank + "+" + length;
				context.write(new Text(adj), new Text(data));
			}
		}
		// Send to self
		context.write(new Text(node), new Text(sections[1]));

	}

}
