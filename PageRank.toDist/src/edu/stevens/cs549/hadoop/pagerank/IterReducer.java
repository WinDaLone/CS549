package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IterReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double d = PageRankDriver.DECAY; // Decay factor
		/*
		 * TODO done: emit key:node+rank, value: adjacency list Use PageRank
		 * algorithm to compute rank from weights contributed by incoming edges.
		 * Remember that one of the values will be marked as the adjacency list
		 * for the node.
		 */
		// Input: key-node value-rank(nodeLinkesTo)+length(nodeLinks)
		// Output: key+rank nodeLinkedTo
		
		/* 
		 * Calculate the rank
		 * PageRank(p) = (1-d) + d * sum(PageRank(b) / N(b))
		 */
		double sum = 0;
		Iterator<Text> iterator = values.iterator();
		String nodeVal = "";
		while (iterator.hasNext()) {
			String data = iterator.next().toString();
			String[] parts = data.split("\\+");
			if (parts.length == 2) {
				double rank = Double.parseDouble(parts[0]);
				double length = Double.parseDouble(parts[1]);
				sum += rank / length;
			} else {
				nodeVal = data; // adj list
			}
		}
		sum = (1 - d) + d * sum;

		String nodeKey = key + "+" + String.valueOf(sum); // key+rank
		context.write(new Text(nodeKey), new Text(nodeVal));

	}
}
