package assignment3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Maps;

import util.StringIntegerList;
import util.StringIntegerList.StringInteger;



/**
 * This class runs through the index twice just to remove uncommon lemmas (features).
 * This is to avoid OOM errors and in general to avoid wasting time on features that aren't informative.
 * 
 * The methods here work similarly to our vector creation methods.
 * 
 *
 */
public class FilterFeatures {
	
	private static final Map<String, Integer> freqDist = Maps.newHashMap();
	private String indexPath;
	
	public FilterFeatures(String indexPath) {
		this.indexPath = indexPath;
	}
	
	public void countFeatures() {
		
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream stream = fs.open(new Path(indexPath));
			
			String line = stream.readLine();
			
			while (line != null) {
				String[] profIndex = line.split("\t");
				StringIntegerList indicesSIL = new StringIntegerList();
				indicesSIL.readFromString(profIndex[1]);
				
				for (StringInteger si: indicesSIL.getIndices()) {
					int count = freqDist.containsKey(si.getString()) ? freqDist.get(si.getString()) : 0;
					freqDist.put(si.getString(), count + 1);
				}
				
				line = stream.readLine();
			}
			
			stream.close();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
	}
	
	public void removeFeatures() {
		
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream stream = fs.open(new Path(indexPath));
			
			Configuration conf2 = new Configuration();
			FileSystem fs2 = FileSystem.get(conf2);
			File file = new File("filteredIndex");
			file.createNewFile();
			BufferedWriter filteredFile = new BufferedWriter(new FileWriter(file));
			
			String line = stream.readLine();
			
			while (line != null) {
				String[] profIndex = line.split("\t");
				StringIntegerList sourceSIL = new StringIntegerList();
				List<StringInteger> dstSIL = new ArrayList<StringInteger>();
				
				sourceSIL.readFromString(profIndex[1]);
				
				for (StringInteger si: sourceSIL.getIndices()) {
					if (freqDist.get(si.getString())>50) {
						dstSIL.add(si);
					}
				}
				
				StringIntegerList filtered = new StringIntegerList(dstSIL);
				
				filteredFile.write(profIndex[0] + "\t" + filtered.toString());
				filteredFile.newLine();
				
				line = stream.readLine();
			}
			
			stream.close();
			filteredFile.flush();
			filteredFile.close();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
	
	
	

}
