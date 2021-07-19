package com.zjm.control;

import com.zjm.model.WebLog;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WebLogReduce extends Reducer<LongWritable, WebLog, NullWritable, Text> {
    public void reduce(LongWritable key,Iterable<WebLog> values,Context context) throws IOException, InterruptedException {
        String webloginfo=null;
        for (WebLog webLog : values) {
            webloginfo=webLog.toString();
            break;
        }
        context.write(NullWritable.get(),new Text(webloginfo));
    }
}
