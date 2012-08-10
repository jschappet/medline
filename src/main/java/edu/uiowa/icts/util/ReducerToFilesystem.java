package edu.uiowa.icts.util;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReducerToFilesystem extends Reducer<Text, Text, Text, Text>{
	
    private static final Logger logger = LoggerFactory.getLogger(ReducerToFilesystem.class);

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
		//logger.info("PMID: " + key);
		for (Text val : values)
			context.write(key, val);
    }
}
