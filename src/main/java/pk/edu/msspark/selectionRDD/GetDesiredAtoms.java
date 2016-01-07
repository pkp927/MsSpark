package pk.edu.msspark.selectionRDD;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import pk.edu.msspark.utils.Vector3D;
import scala.Tuple2;

/* GetDesiredAtoms class to return rdd of desired atoms
 * from the rdd of all atoms
 */
class GetDesiredAtoms implements Function<Tuple2<Integer, Double[]>, Boolean>{
	
	// parameters for desired atoms
	Broadcast<Vector3D> min;
	Broadcast<Vector3D> max;
	Broadcast<Double[]> atomTypes;
	Broadcast<Double[]> atomIds;
	
	// index of atoms data
	Broadcast<Integer> offset;
	Broadcast<Integer> pos;

    public GetDesiredAtoms(Broadcast<Vector3D> mn, Broadcast<Vector3D> mx, Broadcast<Integer> of, Broadcast<Integer> p){
             min = mn;
             max = mx;
             offset = of;
             pos = p;
    }
	
	public Boolean call(Tuple2<Integer, Double[]> t) throws Exception {
		int i = pos.value() - offset.value();
		Vector3D mn = min.value();
		Vector3D mx = max.value();
		// check if atom within desired boundary
		if((t._2[i]>=mn.x) && (t._2[i+1]>=mn.y) && (t._2[i+2]>=mn.z)){
			if((t._2[i]<=mx.x) && (t._2[i+1]<=mx.y) && (t._2[i+2]<=mx.z)){
				return true;
			}
		}
		return false;
	}
	
}