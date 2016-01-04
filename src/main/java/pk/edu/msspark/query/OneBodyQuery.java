package pk.edu.msspark.query;

import java.io.Serializable;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import pk.edu.msspark.resultRDD.VectorRDD;
import pk.edu.msspark.selectionRDD.FrameAtomsRDD;
import pk.edu.msspark.selectionRDD.SelectedAtomsRDD;
import pk.edu.msspark.utils.Vector3D;

public class OneBodyQuery {

	public static VectorRDD getMOI(FrameAtomsRDD f){
		return new VectorRDD(f.getFrameAtomsRDD().mapValues(new ConvertToMOIVec()).reduceByKey(new AddVec()));
	}
	
	public static VectorRDD getMOI(SelectedAtomsRDD f){
		return new VectorRDD(f.getSelectedAtomsRDD().mapValues(new ConvertToMOIVec()).reduceByKey(new AddVec()));
	}
	
}

class AddVec implements Function2<Vector3D, Vector3D, Vector3D>{

	public Vector3D call(Vector3D mc1, Vector3D mc2) throws Exception {
		return mc1.add(mc2);
	}
	
}

class ConvertToMOIVec implements Function<Double[], Vector3D>{

	public Vector3D call(Double[] d) throws Exception {
		Vector3D v = new Vector3D();
		v.x = d[2]*d[6];
		v.y = d[3]*d[6];
		v.z = d[4]*d[6];
		return v;
	}
	
}