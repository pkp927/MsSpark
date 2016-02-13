package pk.edu.msspark.query;

import org.apache.spark.api.java.function.Function;
import pk.edu.msspark.utils.Vector3D;

/* Class to calculate COM for mapValues transformation
 */
public class ConvertToCom implements Function<Double[], Vector3D>{

	public Vector3D call(Double[] d) throws Exception {
		Vector3D v = new Vector3D();;
		v.x = d[0]/d[3];
		v.y = d[1]/d[3];
		v.z = d[2]/d[3];
		return v;
	}

}
