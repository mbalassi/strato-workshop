package hu.sztaki.stratosphere.workshop.batch.outputformat;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.InputTypeConfigurable;
import eu.stratosphere.types.TypeInformation;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.api.java.tuple.Tuple2;

public class ColumnOutputFormat extends FileOutputFormat<Tuple2<Integer,double[]>> implements InputTypeConfigurable {

  private static final long serialVersionUID = 1L;
      
  private final String fieldDelimiter = "|";
  private final String recordDelimiter = "\n";
  private transient Writer wrt;
  private String charsetName;

  public ColumnOutputFormat() { super();}

  public ColumnOutputFormat(String outputPath) {
    super(new Path(outputPath));
  }
  
  public void setCharsetName(String charsetName) {
    this.charsetName = charsetName;
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
    super.open(taskNumber, numTasks);
    this.wrt = this.charsetName == null ? new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096)) :
      new OutputStreamWriter(new BufferedOutputStream(this.stream, 4096), this.charsetName);
  }

  @Override
  public void close() throws IOException {
    if (wrt != null) {
      this.wrt.close();
    }
    super.close();
  }
 
  @Override 
  public void writeRecord(Tuple2<Integer,double[]> record) throws IOException {
    if(record == null) {
      throw new NullPointerException("Record cannot be null!");
    } else {
	
      if(record.f1 == null) {
        throw new NullPointerException("Record's array cannot be null!");
      } else {	    
        double[] elements = record.f1;
        int k = elements.length;
    
        if(k == 0) {
          throw new NullPointerException("The length of vector cannot be 0 at output!");
        } else {
          //writing to output:
          if(record.f0 == null) {
	
	  } else {
	    Object id = record.f0;
            this.wrt.write(id.toString());
            this.wrt.write(this.fieldDelimiter);
            for(int i=0; i<k; i++){
              Object element = elements[i];
	      this.wrt.write(element.toString());
              this.wrt.write(this.fieldDelimiter);
            }
            this.wrt.write(this.recordDelimiter);
	  }
	}
      }
    }
  }

  @Override
  public void setInputType(TypeInformation<?> type) {
    if (!type.isTupleType()) {
      throw new InvalidProgramException("The " + ColumnOutputFormat.class.getSimpleName() +
    	" can only be used to write tuple data sets.");
    }
  }
  

}