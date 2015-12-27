import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class Utils {
	static Gson gson = getGson();
	static Configuration conf = getConf();
	static FileSystem fs = getFileSystem();
	public static Gson getGson() {
		GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeAdapter(SmallRow.class, new SmallRowDeserializer());
		gsonBuilder.registerTypeAdapter(SmallRow.class, new SmallRowSerializer()); 
		//gsonBuilder.registerTypeAdapter(SmallTable.class, new SmallTableDeserializer()); 
		//gsonBuilder.registerTypeAdapter(SmallTable.class, new SmallTableSerializer()); 
		return gsonBuilder.excludeFieldsWithoutExposeAnnotation().create();
	}
	
	public static Configuration getConf() {
		 Configuration conf = new Configuration();
		 conf.addResource("/usr/local/hadoop/etc/hadoop/core-site.xml");
		 conf.addResource("/usr/local/hadoop/etc/hadoop/hdfs-site.xml");
		 conf.set("fs.defaultFS", "hdfs://node11:54310/");
		 conf.set("hadoop.job.ugi", "hadoopuser");
		 conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		 return conf;
	}
	
	public static FileSystem getFileSystem() {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(Utils.conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return fs;
	}
}
