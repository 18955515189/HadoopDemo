package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


/**
 * hadoop java实现代码
 * Created by david on 2018/6/7.
 */
public class HadoopTest {

    //首先实例化一个hdfs的客户端对象
    FileSystem fileSystem = null;
    Configuration conf = null;

    @Before
    public void init() throws IOException {
        conf = new Configuration(  );
        System.setProperty( "HADOOP_USER_NAME","root" );
        conf.set( "fs.defaultFS","hdfs://hadoop01:9000" );
        fileSystem = FileSystem.get( conf );
    }

    @Test
    public void testLs() throws IOException {
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);

        //通过这个迭代器可以遍历出文件
        while( listFiles.hasNext() ){
            LocatedFileStatus next = listFiles.next();
            Path path = next.getPath();
            String name = path.getName();
            System.out.println(" 文件 ："+ name );
        }
    }

    @Test
    public void tesAdd() throws IOException {
        fileSystem.copyFromLocalFile( new Path("d://1.txt"),new Path("/") );
        fileSystem.close();
    }

    @Test
    public void testCopyToLocal() throws IOException {
        fileSystem.copyToLocalFile( new Path("/1.txt"),new Path( "E://" ) );
        fileSystem.close();
    }

}
