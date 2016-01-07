package pk.edu.msspark.utils;

/* Interface to define the file format index
 */

public interface FileFormat {
	
	  public static final int INFO_INDEX = 0;
	  public static final int FRAME_NO_INDEX = 1;
	  // next elements must be after frame no index
	  public static final int ATOM_NO_INDEX = 2;
	  public static final int ATOM_TYPE_INDEX = 3;
	  public static final int POS_VEC_INDEX = 4;
	  public static final int CHARGE_INDEX = 7;
	  public static final int MASS_INDEX = 8;
	  public static final int ROW_LENGTH = 9;
	  
}
