

import java.io.Serializable;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class OneBodyQuery {

	public static VectorRDD getMOI(FrameAtomsRDD f){
		return new VectorRDD(f.getFrameAtomsRDD().mapValues(new ConvertToMOIVec()).reduceByKey(new AddVec()));
	}
	
}

class AddVec implements Function2<VecCalc, VecCalc, VecCalc>{

	public VecCalc call(VecCalc mc1, VecCalc mc2) throws Exception {
		return mc1.add(mc2);
	}
	
}

class ConvertToMOIVec implements Function<Double[], VecCalc>{

	public VecCalc call(Double[] d) throws Exception {
		VecCalc v = new VecCalc();
		v.x = d[2]*d[6];
		v.y = d[3]*d[6];
		v.z = d[4]*d[6];
		return v;
	}
	
}
