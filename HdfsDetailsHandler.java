package com.test.data.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

public class HdfsDetailsHandler {

    public static void main(String args[]) throws IOException {

        String namenodeurl = "hdfs://192.168.43.189:8020";
        String parentPath = "/";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", namenodeurl);
        Path inputPath = new Path(parentPath);
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] parentStatus = fs.listStatus(inputPath);
        System.out.println("--------------------------------------");
        System.out.print("directoryname: " + parentPath);
        ContentSummary hdfsDetails = fs.getContentSummary(inputPath);
        double g = (((hdfsDetails.getSpaceConsumed()/1024.0)/1024.0)/1024.0);
        System.out.print("\tdirectorysize: " + g);
        System.out.print("\tfilecount: " + hdfsDetails.getFileCount()+"\n");
        System.out.println("--------------------------------------");

        getContent(fs,parentStatus,namenodeurl);
    }


    /**
     *
     * @param fs
     * @param parentStatus
     * @param namenodeurl
     * @throws IOException
     */
    public static void getContent(FileSystem fs, FileStatus[] parentStatus,String namenodeurl) throws IOException {

        for (int i = 0; i < parentStatus.length; i++) {
            FileStatus fileStatus = parentStatus[i];


            if (fileStatus.isDirectory()) {
                System.out.print("directoryname: " + fileStatus.getPath().toString().replace(namenodeurl, ""));
                ContentSummary hdfsDetails = fs.getContentSummary(fileStatus.getPath());

                double g = (((hdfsDetails.getSpaceConsumed()/1024.0)/1024.0)/1024.0);
                System.out.print("\tdirectorysize: " + g);
                System.out.print("\tfilecount: " + hdfsDetails.getFileCount()+"\n");
                FileStatus[] subStatus = fs.listStatus(fileStatus.getPath());
                getContent(fs, subStatus, namenodeurl);
            }

        }
    }
}