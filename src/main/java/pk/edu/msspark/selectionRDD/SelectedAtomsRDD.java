package pk.edu.msspark.selectionRDD;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

import pk.edu.msspark.utils.*;

/* SelectedAtomsRDD class to support the Molecular Simulation data
 * which is in the form of frame number paired with the data
 * corresponding to the atoms in the that frame
 */

public class SelectedAtomsRDD implements Serializable, FileFormat{
	
	// default parameters for atom selection
	Broadcast<Integer> firstFrame;
	Broadcast<Integer> lastFrame;
	Broadcast<int[]> skip;
	Broadcast<Vector3D> min;
	Broadcast<Vector3D> max;
	Broadcast<Double[]> atomTypes;
	Broadcast<Double[]> atomIds;
	
	// file format 
	Broadcast<Integer> info_index;
	Broadcast<Integer> frame_no_index;
	Broadcast<Integer> row_length;
	Broadcast<Integer> offset;
	Broadcast<Integer> pos;
	
	// java pair rdd is used to represent the selected atoms corresponding to the frame number 
	private JavaPairRDD<Integer, Double[]> selection;
	
	// constructor to create RDD of the desired atoms	
	public SelectedAtomsRDD(JavaSparkContext sc, String inputLocation, SelectParameters param){

		// broadcast the variables
		broadcastVar(sc, param);
		
		// set the RDD of desired atoms
		/*this.setSelectedAtomsRDD(sc.textFile(inputLocation)
				.filter(new GetDesiredAtomsData(info_index, frame_no_index, row_length, firstFrame, lastFrame, skip))
				.mapToPair((new FrameAtomsMapper(frame_no_index, row_length))));
		*/
		this.setSelectedAtomsRDD(sc.textFile(inputLocation).
				filter(new GetAllAtomsData(info_index)).
				mapToPair(new FrameAtomsMapper(frame_no_index, offset, row_length)).
				filter(new GetDesiredFramesAtoms(firstFrame, lastFrame, skip, min, max, offset, pos)));
	}
	
	// constructor to create RDD of the desired atoms	
	public SelectedAtomsRDD(JavaSparkContext sc, FrameAtomsRDD far, SelectParameters param){
		
			// broadcast the variables
			broadcastVar(sc, param);
			
			// set the RDD of desired atoms
			this.setSelectedAtomsRDD(far.getFrameAtomsRDD().
					filter(new GetDesiredFramesAtoms(firstFrame, lastFrame, skip, min, max, offset, pos)));
			
	}
	
	private void broadcastVar(JavaSparkContext sc, SelectParameters param){

		// broadcast the parameters
		firstFrame = sc.broadcast(param.firstFrame);
		lastFrame = sc.broadcast(param.lastFrame);
		if(!param.skip.isEmpty()){
			String[] splitted = param.skip.split("\\s+");
			int[] sk = new int[splitted.length];
			for(int i=0; i<splitted.length; i++){
				sk[i] = Integer.parseInt(splitted[i]);
			}
			skip = sc.broadcast(sk);
		}else{
			skip = sc.broadcast(null);
		}
		if(param.minBound != null){
			min = sc.broadcast(param.minBound);
		}else{
			min = sc.broadcast(null);
		}
		if(param.maxBound != null){
			max = sc.broadcast(param.maxBound);
		}else{
			max = sc.broadcast(null);
		}
		
		// broadcast file format
		info_index = sc.broadcast(FileFormat.INFO_INDEX);
		frame_no_index = sc.broadcast(FileFormat.FRAME_NO_INDEX);
		row_length = sc.broadcast(FileFormat.ROW_LENGTH);
		offset = sc.broadcast(FileFormat.FRAME_NO_INDEX + 1);
		pos = sc.broadcast(FileFormat.POS_VEC_INDEX);
	}
	
	// setter of RDD
	public void setSelectedAtomsRDD(JavaPairRDD<Integer, Double[]> s){
		this.selection = s;
	}
	
	// getter of RDD
	public JavaPairRDD<Integer, Double[]> getSelectedAtomsRDD(){
		return this.selection;
	}

}


