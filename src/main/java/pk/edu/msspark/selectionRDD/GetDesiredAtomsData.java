package pk.edu.msspark.selectionRDD;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

/* GetDesiredAtomsData class to filter the 
 * atoms data of desired frames out of raw 
 * data in the file
 */
class GetDesiredAtomsData implements Function<String, Boolean> {
	
	  // index that tells about atom or frame data
	  Broadcast<Integer> info_index;
	  Broadcast<Integer> frame_no_index;
	  Broadcast<Integer> row_length;
	  
	  // parameters for desired frame
	  Broadcast<Integer> firstFrame;
	  Broadcast<Integer> lastFrame;
	  Broadcast<int[]> skip;
	  
	  public GetDesiredAtomsData(Broadcast<Integer> i, Broadcast<Integer> fn, Broadcast<Integer> rl,  Broadcast<Integer> f, Broadcast<Integer> l, Broadcast<int[]> sk){
		  info_index = i;
		  frame_no_index = fn;
		  row_length = rl;
		  firstFrame = f;
          lastFrame = l;
          skip = sk;
	  }
	
	  public Boolean call(String s) { 
		  String[] splitted = s.split("\\s+");
		  // check info index
		  if(splitted[info_index.value()].equals("ATOM")){
			  // check if desired frame 
			  int frameNo = Integer.parseInt(splitted[frame_no_index.value()]);
			  if(frameNo >= firstFrame.value() && frameNo <= lastFrame.value()){
				  int[] a = skip.value();
					if( a != null )
					for(int i = 0; i < a.length; i++){
						if(frameNo == a[i]){
							return false;
						}
					}
					return true;
			  }
		  }
		  return false;
	  }
}