package com.zjm.model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.util.HashMap;

public class WebLog implements Writable {
    private String ip;
    private int year;
    private int month;
    private int date;
    private int hour;
    private int minutes;
    private String requestMethod;
    private String requestUrl;
    private String requestAgree;
    private int responesNum;
    private int dataSize;
    private String webOrigin;
    private String browserInfo;

    public WebLog(){

    }
    public WebLog(String ip, String time, String request, int responesNum, int dataSize, String webOrigin, String browserInfo) {
        this.ip = ip;
        this.responesNum = responesNum;
        this.dataSize = dataSize;
        this.webOrigin = webOrigin;
        this.browserInfo = browserInfo;
        splitTime(time);
        splitRequest(request);
    }

    private void splitRequest(String request) {
        String[] str = request.split("\\s+");
        this.requestMethod=str[0];
        this.requestUrl=str[1];
        this.requestAgree=str[2];
    }
    public void splitTime(String time){
        //22/Jan/2019:03:56:14 +0330
        this.date=Integer.parseInt(time.substring(0,time.indexOf('/')));
        time=time.substring(time.indexOf('/')+1);
        HashMap<String, Integer> monthMap = new HashMap<String, Integer>();
        monthMap.put("Jan",1);
        monthMap.put("Feb",2);
        monthMap.put("Mar",3);
        monthMap.put("Apr",4);
        monthMap.put("May",5);
        monthMap.put("Jun",6);
        monthMap.put("Jul",7);
        monthMap.put("Aug",8);
        monthMap.put("Sep",9);
        monthMap.put("Oct",10);
        monthMap.put("Nov",11);
        monthMap.put("Dec",12);
        String sMonth=time.substring(0,time.indexOf('/'));
        this.month=monthMap.get(sMonth);
        time=time.substring(time.indexOf('/')+1);
        this.year=Integer.parseInt(time.substring(0,time.indexOf(':')));
        time=time.substring(time.indexOf(':')+1);
        this.hour=Integer.parseInt(time.substring(0,time.indexOf(':')));
        time=time.substring(time.indexOf(':')+1);
        this.minutes=Integer.parseInt(time.substring(0,time.indexOf(':')));
    }
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ip);
        dataOutput.writeInt(year);
        dataOutput.writeInt(month);
        dataOutput.writeInt(date);
        dataOutput.writeInt(hour);
        dataOutput.writeInt(minutes);
        dataOutput.writeInt(responesNum);
        dataOutput.writeInt(dataSize);
        dataOutput.writeUTF(requestMethod);
        dataOutput.writeUTF(requestUrl);
        dataOutput.writeUTF(requestAgree);
        dataOutput.writeUTF(webOrigin);
        dataOutput.writeUTF(browserInfo);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.ip=dataInput.readUTF();
        this.year=dataInput.readInt();
        this.month=dataInput.readInt();
        this.date=dataInput.readInt();
        this.hour=dataInput.readInt();
        this.minutes=dataInput.readInt();
        this.responesNum=dataInput.readInt();
        this.dataSize=dataInput.readInt();
        this.requestMethod=dataInput.readUTF();
        this.requestUrl=dataInput.readUTF();
        this.requestAgree=dataInput.readUTF();
        this.webOrigin=dataInput.readUTF();
        this.browserInfo=dataInput.readUTF();
    }

    @Override
    public String toString() {
        return  ip  +
                "," + year +
                "," + month +
                "," + date +
                "," + hour +
                "," + minutes +
                "," + requestMethod  +
                "," + requestUrl  +
                "," + requestAgree  +
                "," + responesNum +
                "," + dataSize +
                "," + webOrigin  +
                "," + browserInfo ;
    }
}
