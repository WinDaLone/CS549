package edu.stevens.cs549.hadoop.pagerank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinReducer extends Reducer<DoubleWritable, Text, Text, Text> {
	private Map<Long, Text> map = new HashMap<>();

	@Override
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		/*
		 * TODO done: For each value, emit: key:value, value:-rank
		 */
		// Input: key-rank value-node
		// Output: nodeID nodeURL rank
		Iterator<Text> iterator = values.iterator();
		String rank = String.valueOf(key.get());
		while (iterator.hasNext()) {
			Text text = iterator.next();
			long nodeKey = Long.parseLong(text.toString());
			if (map.containsKey(nodeKey)) {
				context.write(map.get(nodeKey), new Text(rank));
			} else {
				context.write(text, new Text(rank));
			}
		}
	}

	@Override
	protected void setup(Reducer<DoubleWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO done map key name to hashMap
		super.setup(context);
		URI[] files = context.getCacheFiles();
		System.out.println("FinReducer setup: " + files.length);
		Path path = new Path(files[0]);
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(files[0], configuration);
		// Iterator through the path and read all the node data into the HashMap
		if (fileSystem.exists(path)) {
			FileStatus[] lStatus = fileSystem.listStatus(path);
			for (FileStatus fs : lStatus) {
				FSDataInputStream inputStream = fileSystem.open(fs.getPath());
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
				String line = "";
				while ((line = reader.readLine()) != null) {
					String[] words = line.split(":");
					long key = Long.parseLong(words[0].trim());
					map.put(key, new Text(line));
				}
				reader.close();
			}
			fileSystem.close();
		}
	}

}
