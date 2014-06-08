/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package hu.sztaki.stratosphere.workshop.batch.als;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.InputTypeConfigurable;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.TypeInformation;
import hu.sztaki.stratosphere.workshop.batch.customals.MatrixLine;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

public class ColumnOutputFormat extends FileOutputFormat<Tuple2<Integer, double[]>> implements
		InputTypeConfigurable {

	private static final long serialVersionUID = 1L;

	private final String fieldDelimiter = "|";
	private final String recordDelimiter = "\n";
	private transient Writer wrt;
	private String charsetName;

	public ColumnOutputFormat() {
		super();
	}

	public ColumnOutputFormat(String outputPath) {
		super(new Path(outputPath));
	}

	public void setCharsetName(String charsetName) {
		this.charsetName = charsetName;
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		this.wrt = this.charsetName == null ? new OutputStreamWriter(new BufferedOutputStream(
				this.stream, 4096)) : new OutputStreamWriter(new BufferedOutputStream(this.stream,
				4096), this.charsetName);
	}

	@Override
	public void close() throws IOException {
		if (wrt != null) {
			this.wrt.close();
		}
		super.close();
	}

	@Override
	public void writeRecord(Tuple2<Integer, double[]> line) throws IOException {
		if (line == null) {
			throw new NullPointerException("Record cannot be null!");
		} else {

			if (line.f1 == null) {
				throw new NullPointerException("Record's array cannot be null!");
			} else {
				double[] elements = line.f1;
				int k = elements.length;

				if (k == 0) {
					throw new NullPointerException("The length of vector cannot be 0 at output!");
				} else {
					// writing to output:
					if (line.f0 == null) {

					} else {
						Object id = line.f0;
						this.wrt.write(id.toString());
						this.wrt.write(this.fieldDelimiter);
						for (int i = 0; i < k; i++) {
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
			throw new InvalidProgramException("The " + ColumnOutputFormat.class.getSimpleName()
					+ " can only be used to write tuple data sets.");
		}
	}
}
