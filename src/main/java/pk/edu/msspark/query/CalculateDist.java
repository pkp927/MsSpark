package pk.edu.msspark.query;

import org.apache.spark.api.java.function.Function;
import pk.edu.msspark.utils.Vector3D;
import scala.Tuple2;

/* Class to calculate distance between two points for mapValues transformation
 */
class CalculateDist implements Function<Tuple2<Vector3D,Vector3D>,Double>{
	
	public Double euclideanDistance(Vector3D v1, Vector3D v2){
		Double x = Math.abs(v1.x - v2.x);
		Double y = Math.abs(v1.y - v2.y);
		Double z = Math.abs(v1.z - v2.z);
		return Math.sqrt(x*x+y*y+z*z);
	}
	
	public Double call(Tuple2<Vector3D, Vector3D> t) throws Exception {
		return euclideanDistance(t._1, t._2);
	}
}