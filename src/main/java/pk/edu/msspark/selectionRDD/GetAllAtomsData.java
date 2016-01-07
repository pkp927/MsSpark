package pk.edu.msspark.selectionRDD;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

/* GetAllAtomsData class to filter the atoms data 
 * out of raw data in the file
 */
class GetAllAtomsData implements Function<String, Boolean> {
	
	  // index that tells about atom or frame data
	  Broadcast<Integer> info_index;
	  
	  public GetAllAtomsData(Broadcast<Integer> i){
		  info_index = i;
	  }
	
	  public Boolean call(String s) { 
		  String[] splitted = s.split("\\s+");
		  // check for info index
		  if(splitted[info_index.value()].equals("ATOM")){
			  return true;
		  }
		  return false;
	  }
}