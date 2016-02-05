package com.saggezza.mykafka;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import java.net.URI;
import java.net.URISyntaxException;


/**
 * Created by subodhkumar on 29/9/15.
 */
public class MyHDFSFileCreation {

    private  Configuration configuration;
    private FileSystem hdfs;
    private  Path newFolderPath;
    private FSDataOutputStream out;
    private BufferedWriter br;
    private final String hadoop_core=System.getProperty("hadoop_core");
    private final String Hadoop_conf=System.getProperty("hadoop_conf");
    private final String hdfs_uri=System.getProperty("hdfs_uri");
    private final String hdfsinput_path=System.getProperty("hdfsinput_path");
    private  MyHDFSFileCreation() throws IOException, URISyntaxException {

        configuration = new Configuration();

        configuration.addResource(new Path(hadoop_core));
        configuration.addResource(new Path(Hadoop_conf));
        hdfs = FileSystem.get(new URI(hdfs_uri), configuration);
        newFolderPath = new Path(hdfsinput_path);
        //newFolderPath = Path.mergePaths(workingDir, newFolderPath);
        //hdfs.setReplication(newFolderPath, (short) 1);
        if (hdfs.exists(newFolderPath))

        {
            System.out.println(hdfs.exists(newFolderPath));

            hdfs.delete(newFolderPath, true); //Delete existing Directory
        }
        out = hdfs.create(newFolderPath);
        br = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));

    }
    public void  append(String line) throws IOException {
        br.write(line+"\n");

    }
    public void close() throws IOException {

        br.close();
        out.close();
        hdfs.close();
    }
    public static MyHDFSFileCreation creat() throws IOException, URISyntaxException {
        return new MyHDFSFileCreation();
    }
}