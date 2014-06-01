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

package hu.sztaki.stratosphere.workshop.streaming.wordcount;

import java.io.BufferedReader;
import java.io.FileReader;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class WordCountSourceSplitter extends UserSourceInvokable {

	private BufferedReader br = null;
	private String line = new String();
	private StreamRecord outRecord = new StreamRecord(new Tuple1<String>());

	@Override
	public void invoke() throws Exception {
		br = new BufferedReader(new FileReader(
				"src/test/resources/testdata/hamlet.txt"));
		while (true) {
			line = br.readLine();
			if (line == null) {
				break;
			}
			if (line != "") {
				line=line.replaceAll("[\\-\\+\\.\\^:,]", "");
				for (String word : line.split(" ")) {
					outRecord.setString(0, word);
					System.out.println("word=" + word);
					emit(outRecord);
					performanceCounter.count();
				}
			}
		}
	}
}