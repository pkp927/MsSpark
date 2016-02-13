package pk.edu.msspark.query;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import pk.edu.msspark.utils.Vector3D;

/* Class to extract DM required attributes for mapValues transformation
 */
public class ConvertToDMVec implements Function<Double[], Vector3D>{

	// file format 
	Broadcast<Integer> offset;
	Broadcast<Integer> pos;
	Broadcast<Integer> charge_index;
	
	public ConvertToDMVec(Broadcast<Integer> of, Broadcast<Integer> p, Broadcast<Integer> ci){
		offset = of;
		pos = p;
		charge_index = ci;
	}
	
	public Vector3D call(Double[] d) throws Exception {
		Vector3D v = new Vector3D();
		int p = pos.value() - offset.value();
		int m = charge_index.value() - offset.value();
		v.x = d[p]*d[m];
		v.y = d[p+1]*d[m];
		v.z = d[p+2]*d[m];
		return v;
	}

}