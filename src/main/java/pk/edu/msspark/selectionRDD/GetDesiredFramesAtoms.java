package pk.edu.msspark.selectionRDD;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import pk.edu.msspark.utils.Vector3D;
import scala.Tuple2;

/* GetDesiredFrameAtoms class to return rdd of desired atoms
 * and frames from given pair rdd of all frames and atoms.
 * It is a combination of GetDesiredFrames and GetDesiredAtoms.
 */
class GetDesiredFramesAtoms implements Function<Tuple2<Integer, Double[]>, Boolean>{

	// parameters for desired data
	Broadcast<Integer> firstFrame;
	Broadcast<Integer> lastFrame;
	Broadcast<int[]> skip;
	Broadcast<Vector3D> min;
	Broadcast<Vector3D> max;
	Broadcast<Double[]> atomTypes;
	Broadcast<Double[]> atomIds;
	
	// index of data
	Broadcast<Integer> offset;
	Broadcast<Integer> pos;

    public GetDesiredFramesAtoms(Broadcast<Integer> f, Broadcast<Integer> l, 
    		Broadcast<int[]> sk, Broadcast<Vector3D> mn, Broadcast<Vector3D> mx,
    		Broadcast<Integer> of, Broadcast<Integer> p){
              firstFrame = f;
              lastFrame = l;
              skip = sk;
              min = mn;
              max = mx;
              offset = of;
              pos = p;
    }
	
	public Boolean call(Tuple2<Integer, Double[]> t) throws Exception {
		// check if desired frame
		if(t._1 >= firstFrame.value() && t._1 <= lastFrame.value()){
			int[] a = skip.value();
			if( a != null ){
				for(int i = 0; i < a.length; i++){
					if(t._1 == a[i]){
						return false;
					}
				}
			}
			// check if desired atom
			Vector3D mn = min.value();
			Vector3D mx = max.value();
			int i = pos.value() - offset.value();
			if(mn != null){
				if((t._2[i]<mn.x) && (t._2[i+1]<mn.y) && (t._2[i+2]<mn.z)){
					return false;
				}
			}
			if(mx != null){
				if((t._2[i]>mx.x) && (t._2[i+1]>mx.y) && (t._2[i+2]>mx.z)){
					return false;
				}
			}
			return true;
		}
		return false;
	}
	
}