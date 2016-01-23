package pk.edu.msspark.query;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import pk.edu.msspark.resultRDD.*;
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
	Broadcast<Integer> charge_index;
	// spark context object
	JavaSparkContext sc;
	
	/* Constructor that broadcasts the required variables
	 */
	public OneBodyQuery(JavaSparkContext sc){
		this.sc = sc;
		offset = sc.broadcast(FileFormat.FRAME_NO_INDEX + 1);
		pos = sc.broadcast(FileFormat.POS_VEC_INDEX);
		mass_index = sc.broadcast(FileFormat.MASS_INDEX);
		charge_index = sc.broadcast(FileFormat.CHARGE_INDEX);
	}
	
	/* Function to calculate MOI 
	 */
	public VectorRDD getMOI(FrameAtomsRDD f){		
		return new VectorRDD(f.getFrameAtomsRDD()
				.mapValues(new ConvertToMoiVec(offset, pos, mass_index))
				.reduceByKey(new AddVector()));
	}
	
	/* Function to calculate MOI 
	 */
	public VectorRDD getMOI(SelectedAtomsRDD f){
		return new VectorRDD(f.getSelectedAtomsRDD()
				.mapValues(new ConvertToMoiVec(offset, pos, mass_index))
				.reduceByKey(new AddVector()));
	}
	
	/* Function to calculate SOM 
	 */
	public ScalarRDD getSOM(FrameAtomsRDD f){		
		return new ScalarRDD(f.getFrameAtomsRDD()
				.mapValues(new ConvertToSomScalar(offset, mass_index))
				.reduceByKey(new AddScalar()));
	}
	
	/* Function to calculate SOM 
	 */
	public ScalarRDD getSOM(SelectedAtomsRDD f){
		return new ScalarRDD(f.getSelectedAtomsRDD()
				.mapValues(new ConvertToSomScalar(offset, mass_index))
				.reduceByKey(new AddScalar()));
	}
	
	/* Function to calculate COM 
	 */
	public VectorRDD getCOM(FrameAtomsRDD f){		
		return new VectorRDD(f.getFrameAtomsRDD()
				.mapValues(new ConvertToComArray(offset, pos, mass_index))
				.reduceByKey(new AddArray())
				.mapValues(new ConvertToCom()));
	}
	
	/* Function to calculate COM 
	 */
	public VectorRDD getCOM(SelectedAtomsRDD f){
		return new VectorRDD(f.getSelectedAtomsRDD()
				.mapValues(new ConvertToComArray(offset, pos, mass_index))
				.reduceByKey(new AddArray())
				.mapValues(new ConvertToCom()));
	}
	
	/* Function to calculate DM
	 */
	public VectorRDD getDM(FrameAtomsRDD f){		
		return new VectorRDD(f.getFrameAtomsRDD()
				.mapValues(new ConvertToDMVec(offset, pos, charge_index))
				.reduceByKey(new AddVector()));
	}
	
	/* Function to calculate DM 
	 */
	public VectorRDD getDM(SelectedAtomsRDD f){
		return new VectorRDD(f.getSelectedAtomsRDD()
				.mapValues(new ConvertToDMVec(offset, pos, charge_index))
				.reduceByKey(new AddVector()));
	}
	
	/* Function to calculate ROG
	 */
	public ScalarRDD getROG(FrameAtomsRDD f, String ax){
		int a;
		if(ax.equals("x")) a = 0;
		else if(ax.equals("y")) a = 1;
		else a = 2;	
		Broadcast<Integer> axis = sc.broadcast(a);
		return new ScalarRDD(f.getFrameAtomsRDD()
				.mapValues(new ConvertToRogArray(offset, pos, mass_index, axis))
				.reduceByKey(new AddArray())
				.mapValues(new ConvertToRog()));
	}
	
	/* Function to calculate ROG 
	 */
	public ScalarRDD getROG(SelectedAtomsRDD f, String ax){
		int a;
		if(ax.equals("x")) a = 0;
		else if(ax.equals("y")) a = 1;
		else a = 2;
		Broadcast<Integer> axis = sc.broadcast(a);
		return new ScalarRDD(f.getSelectedAtomsRDD()
				.mapValues(new ConvertToRogArray(offset, pos, mass_index, axis))
				.reduceByKey(new AddArray())
				.mapValues(new ConvertToRog()));
	}
	
}



