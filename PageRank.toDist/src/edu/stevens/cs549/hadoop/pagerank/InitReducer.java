package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/*
		 * TODO done: Output key: node+rank, value: adjacency list
		 */
		// Input: key-node value-nodeLinkedTo
		// Output: node+rank nodeLinkedTo
		String nodeKey = key.toString();
		nodeKey = nodeKey + "+1";
		StringBuffer nodeVal = new StringBuffer();
		Iterator<Text> iter = values.iterator();
		while (iter.hasNext()) {
			nodeVal.append(iter.next().toString());
			nodeVal.append(" ");
		}
		context.write(new Text(nodeKey), new Text(nodeVal.toString().trim()));
	}
}
