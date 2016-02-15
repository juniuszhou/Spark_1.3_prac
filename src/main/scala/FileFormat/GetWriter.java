package FileFormat;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class GetWriter {
    public static Configuration conf = new Configuration();
    public static String path = "/home/junius/data/mapfile";
    public static Path mapFile = new Path(path);
    public static void write() throws Exception {
       //        writer.append(new IntWritable(1),new Text("value1"));
       // IOUtils.closeStream(writer);//关闭write流
    }

    public static MapFile.Writer getWriter() throws Exception {
        MapFile.Writer.Option keyClass = MapFile.Writer.keyClass(IntWritable.class);
        SequenceFile.Writer.Option valueClass = MapFile.Writer.valueClass(Text.class);
        MapFile.Writer writer = new MapFile.Writer(conf,mapFile,keyClass,valueClass);
        return writer;
    }

    public static MapFile.Writer getWriter(int i) throws Exception {
        Path mapFile = new Path(path + "/part-r-0000" + i);
        MapFile.Writer.Option keyClass = MapFile.Writer.keyClass(IntWritable.class);
        SequenceFile.Writer.Option valueClass = MapFile.Writer.valueClass(Text.class);
        MapFile.Writer writer = new MapFile.Writer(conf,mapFile,keyClass,valueClass);
        return writer;
    }
}
