package pk.edu.msspark.selectionRDD;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import pk.edu.msspark.utils.Vector3D;
import scala.Tuple2;

/* GetDesiredFrames class to return rdd of desired frames
 * from given pair rdd of all frames
 */
class GetDesiredFrames implements Function<Tuple2<Integer, Double[]>, Boolean>{

	// parameters for desired frames
	Broadcast<Integer> firstFrame;
	Broadcast<Integer> lastFrame;
	Broadcast<int[]> skip;

    public GetDesiredFrames(Broadcast<Integer> f, Broadcast<Integer> l, Broadcast<int[]> sk){
              firstFrame = f;
              lastFrame = l;
              skip = sk;
    }
	
	public Boolean call(Tuple2<Integer, Double[]> t) throws Exception {
		// check for desired frame
		if(t._1 >= firstFrame.value() && t._1 <= lastFrame.value()){
			int[] a = skip.value();
			if( a != null ){
				for(int i = 0; i < a.length; i++){
					if(t._1 == a[i]){
						return false;
					}
				}
			}
			return true;
		}
		return false;
	}
	
}