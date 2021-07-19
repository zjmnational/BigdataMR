package com.zjm.control;

import com.zjm.model.WebLog;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class WebLogMap extends Mapper<LongWritable,Text, LongWritable, WebLog> {
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String line = new String(value.getBytes(),0,value.getLength(),"GBK");
        String ip=line.substring(0,line.indexOf(" "));
        line=line.substring(line.indexOf('[')+1);
        String time=line.substring(0,line.indexOf(']'));
        line=line.substring(line.indexOf('\"')+1);
        String request=line.substring(0,line.indexOf('\"')).replace(",","");
        line=line.substring(line.indexOf('\"')+2);
        int responesNum=Integer.parseInt(line.substring(0,line.indexOf(' ')));
        line=line.substring(line.indexOf(' ')+1);
        int dataSize=Integer.parseInt(line.substring(0,line.indexOf(' ')));
        line=line.substring(line.indexOf('\"')+1);
        String webOrigin=line.substring(0,line.indexOf('\"')).replace(",","");
        line=line.substring(line.indexOf('\"')+3);
        String browserInfo=line.substring(0,line.indexOf('\"')).replace(',',' ');
        WebLog webLog = new WebLog(ip, time, request, responesNum, dataSize, webOrigin, browserInfo);
        context.write(key,webLog);

    }
}
