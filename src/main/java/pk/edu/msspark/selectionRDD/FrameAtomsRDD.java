package pk.edu.msspark.selectionRDD;

import java.io.Serializable;

import org.apache.spark.api.java.*;
import org.apache.spark.broadcast.Broadcast;

import pk.edu.msspark.utils.*;

/* FrameAtomsRDD class to support the Molecular Simulation data
 * which is in the form of frame number paired with the data
 * corresponding to the atoms in the that frame
 */

public class FrameAtomsRDD implements Serializable, FileFormat{

	// java pair rdd is used to represent the mapping
	private JavaPairRDD<Integer, Double[]> frames;
	
	// broadcast variables
	Broadcast<Integer> info_index;
	Broadcast<Integer> frame_no_index;
	Broadcast<Integer> row_length;
	Broadcast<Integer> offset;

	// constructor to create rdd from file
	public FrameAtomsRDD(JavaSparkContext sc, String inputLocation){
		
		// broadcast the file format
		info_index = sc.broadcast(INFO_INDEX);
		frame_no_index = sc.broadcast(FRAME_NO_INDEX);
		offset = sc.broadcast(FRAME_NO_INDEX + 1);
		row_length = sc.broadcast(ROW_LENGTH);
		
		// get the frame atoms rdd
		this.setFrameAtomsRDD(sc.textFile(inputLocation)
				.filter(new GetAllAtomsData(info_index))
				.mapToPair(new FrameAtomsMapper(frame_no_index, offset, row_length)));
	}
	
	// setter function to create rdd from existing rdd
	public void setFrameAtomsRDD(JavaPairRDD<Integer, Double[]> f){
		this.frames = f;
	}

    // getter function to get the rdd
	public JavaPairRDD<Integer, Double[]> getFrameAtomsRDD(){
		return this.frames;
	}
	
}

