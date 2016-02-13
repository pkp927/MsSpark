package pk.edu.msspark.query;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import pk.edu.msspark.utils.Vector3D;
import scala.Tuple2;

/* Class to get desired frame data from the given pairRDD
 */
class GetFrame implements Function<Tuple2<Integer, Vector3D>, Boolean>{
	
	Broadcast<Integer> frame;
	
	public GetFrame(Broadcast<Integer> frame){
		this.frame = frame;
	}
	
	public Boolean call(Tuple2<Integer, Vector3D> t) throws Exception {
		if(t._1.equals(frame.value())){  return true;}
		return false;
	}
}