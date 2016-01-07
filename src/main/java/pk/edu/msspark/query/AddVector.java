package pk.edu.msspark.query;

import org.apache.spark.api.java.function.Function2;

import pk.edu.msspark.utils.Vector3D;

class AddVector implements Function2<Vector3D, Vector3D, Vector3D>{

	public Vector3D call(Vector3D mc1, Vector3D mc2) throws Exception {
		return mc1.add(mc2);
	}
	
}