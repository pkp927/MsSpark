package pk.edu.msspark.utils;

/* SelectParameters class to combine parameters
 * required to define the selection of desired
 * data.
 */
public class SelectParameters {
	
	public int firstFrame;
	public int lastFrame;
	public String skip;
	public Vector3D minBound;
	public Vector3D maxBound;
	public String atomType;
	public String atomId;
	public Boolean cached = true;
	
}
