package pk.edu.msspark.query;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import pk.edu.msspark.resultRDD.VectorRDD;
import pk.edu.msspark.selectionRDD.FrameAtomsRDD;
import pk.edu.msspark.selectionRDD.SelectedAtomsRDD;
import pk.edu.msspark.utils.*;

/* OneBodyQuery class implements one body queries
 * for Molecular Simulation data
 */

public class OneBodyQuery {

	// file format 
	Broadcast<Integer> offset;
	Broadcast<Integer> pos;
	Broadcast<Integer> mass_index;
	
	// constructor that broadcasts the required variables
	public OneBodyQuery(JavaSparkContext sc){
		offset = sc.broadcast(FileFormat.FRAME_NO_INDEX + 1);
		pos = sc.broadcast(FileFormat.POS_VEC_INDEX);
		mass_index = sc.broadcast(FileFormat.MASS_INDEX);
	}
	
	public VectorRDD getMOI(FrameAtomsRDD f){		
		return new VectorRDD(f.getFrameAtomsRDD()
				.mapValues(new ConvertToMOIVec(offset, pos, mass_index))
				.reduceByKey(new AddVector()));
	}
	
	public VectorRDD getMOI(SelectedAtomsRDD f){
		return new VectorRDD(f.getSelectedAtomsRDD()
				.mapValues(new ConvertToMOIVec(offset, pos, mass_index))
				.reduceByKey(new AddVector()));
	}
	
}



