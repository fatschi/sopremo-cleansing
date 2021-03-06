/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.io.IOException;
import java.math.BigDecimal;

import org.junit.Test;

import eu.stratosphere.meteor.MeteorParseTest;
import eu.stratosphere.sopremo.CoreFunctions;
import eu.stratosphere.sopremo.cleansing.RecordLinkage;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection;
import eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateSelection.SelectionHint;
import eu.stratosphere.sopremo.cleansing.duplicatedection.DuplicateDetectionImplementation;
import eu.stratosphere.sopremo.cleansing.duplicatedection.SortedNeighborhood;
import eu.stratosphere.sopremo.cleansing.similarity.CoercingSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityExpression;
import eu.stratosphere.sopremo.cleansing.similarity.text.LevenshteinSimilarity;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.SopremoPlan;
import eu.stratosphere.sopremo.type.IntNode;

/**
 * @author arv
 */
public class RecordLinkageParseTest extends MeteorParseTest {

	@Test
	public void testNaive() throws IOException {
		final SopremoPlan actualPlan = parseScript("using cleansing;" +
			"$persons = read from 'file:///input1.json';" +
			"$politicians = read from 'file:///input2.json';" +
			"$duplicates = link records $persons, $politicians " +
			"  where levenshtein($persons.name, $politicians.fullName) >= 0.7;" +
			"write $duplicates to 'file:///output.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input1 = new Source("file:/input1.json");
		final Source input2 = new Source("file:/input2.json");
		final RecordLinkage duplicateDetection = new RecordLinkage().
			withInputs(input1, input2).
			withComparison(getComparison());

		final Sink output = new Sink("file:/output.json").withInputs(duplicateDetection);
		expectedPlan.setSinks(output);

		assertPlanEquals(expectedPlan, actualPlan);
	}

	private CandidateComparison getComparison() {
		final SimilarityExpression similarityExpression = new SimilarityExpression(
			new ObjectAccess("name"),
			new CoercingSimilarity(new LevenshteinSimilarity()),
			new ObjectAccess("fullName"));
		return new CandidateComparison().
			withDuplicateExpression(
			new ComparativeExpression(similarityExpression,
				ComparativeExpression.BinaryOperator.GREATER_EQUAL,
				new ConstantExpression(new BigDecimal("0.7"))));
	}

	@Test
	public void testBlocking() throws IOException {
		final SopremoPlan actualPlan = parseScript("using cleansing;"
			+ "$persons = read from 'file:///input1.json';"
			+ "$politicians = read from 'file:///input2.json';"
			+ "$duplicates = link records $persons, $politicians "
			+ "  partition on {substring($persons.name, 0, 3) : substring($politicians.fullName, 0, 3)}"
			+ "  where levenshtein($persons.name, $politicians.fullName) >= 0.7"
			+ ";"
			+ "write $duplicates to 'file:///output.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input1 = new Source("file:/input1.json");
		final Source input2 = new Source("file:/input2.json");
		final RecordLinkage duplicateDetection =
			new RecordLinkage()
				.withImplementation(DuplicateDetectionImplementation.BLOCKING)
				.withInputs(input1, input2)
				.withComparison(getComparison())
				.withCandidateSelection(
					new CandidateSelection().withSelectionHint(SelectionHint.BLOCK).withPass(
						new FunctionCall(CoreFunctions.SUBSTRING, new ObjectAccess("name"), new ConstantExpression(IntNode.valueOf(0)), new ConstantExpression(IntNode.valueOf(3))),
						new FunctionCall(CoreFunctions.SUBSTRING, new ObjectAccess("fullName"), new ConstantExpression(IntNode.valueOf(0)), new ConstantExpression(IntNode.valueOf(3)))));

		final Sink output = new Sink("file:/output.json")
			.withInputs(duplicateDetection);
		expectedPlan.setSinks(output);

		assertPlanEquals(expectedPlan, actualPlan);
	}
	
	@Test
	public void testSorting() throws IOException {
		final SopremoPlan actualPlan = parseScript("using cleansing;"
			+ "$persons = read from 'file:///input1.json';"
			+ "$politicians = read from 'file:///input2.json';"
			+ "$duplicates = link records $persons, $politicians "
			+ "  sort on {$persons.age : $politicians.ageField}"
			+ "  where levenshtein($persons.name, $politicians.fullName) >= 0.7"
			+ "  with window 20"
			+ ";"
			+ "write $duplicates to 'file:///output.json';");

		final SopremoPlan expectedPlan = new SopremoPlan();
		final Source input1 = new Source("file:/input1.json");
		final Source input2 = new Source("file:/input2.json");
		final RecordLinkage duplicateDetection =
			new RecordLinkage()
				.withImplementation(DuplicateDetectionImplementation.SNM)
				.withInputs(input1, input2)
				.withComparison(getComparison())
				.withCandidateSelection(
					new CandidateSelection().withSelectionHint(SelectionHint.SORT).withPass(
						new ObjectAccess("age"), new ObjectAccess("ageField")));
		((SortedNeighborhood) duplicateDetection.getAlgorithm()).setWindowSize(20);

		final Sink output = new Sink("file:/output.json")
			.withInputs(duplicateDetection);
		expectedPlan.setSinks(output);

		assertPlanEquals(expectedPlan, actualPlan);
	}
}
