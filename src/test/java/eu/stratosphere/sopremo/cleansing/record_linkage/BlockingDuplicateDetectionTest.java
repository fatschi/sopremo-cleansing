package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.sopremo.cleansing.duplicatedection.*;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.JsonUtil;

/**
 * Tests {@link Blocking} within one data source.
 * 
 * @author Arvid Heise
 */
public class BlockingDuplicateDetectionTest extends DuplicateDetectionTestBase<Blocking> {
	private final EvaluationExpression[] blockingKeys;

	/**
	 * Initializes NaiveRecordLinkageInterSourceTest with the given parameter
	 * 
	 * @param projection
	 * @param useId
	 * @param blockingKeys
	 */
	public BlockingDuplicateDetectionTest(final EvaluationExpression projection,
			final boolean useId, final String[][] blockingKeys) {
		super(projection, useId);

		this.blockingKeys = new EvaluationExpression[blockingKeys[0].length];
		for (int index = 0; index < this.blockingKeys.length; index++)
			this.blockingKeys[index] = new ObjectAccess(blockingKeys[0][index]);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.cleansing.record_linkage.DuplicateDetectionTestBase#getCandidateSelection()
	 */
	@Override
	protected CandidateSelection getCandidateSelection() {
		final CandidateSelection candidateSelection = super.getCandidateSelection();
		for (EvaluationExpression blockingKey : this.blockingKeys)
			candidateSelection.withPass(blockingKey);
		return candidateSelection;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.record_linkage.DuplicateDetectionTestBase#generateExpectedPairs(java.util.List,
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.PairFilter,
	 * eu.stratosphere.sopremo.cleansing.duplicatedection.CandidateComparison)
	 */
	@Override
	protected void generateExpectedPairs(List<IJsonNode> input, PairFilter pairFilter, CandidateComparison comparison) {
		BooleanExpression preselect;
		if (pairFilter.isConfigured())
			preselect = pairFilter;
		else
			preselect = new NodeOrderSelector(input);

		final BooleanExpression condition = comparison.asCondition();
		for (final IJsonNode left : input) {
			for (final IJsonNode right : input) {
				for (int index = 0; index < this.blockingKeys.length; index++)
					if (this.blockingKeys[index].evaluate(left).equals(this.blockingKeys[index].evaluate(right))) {
						final IArrayNode<IJsonNode> pair = JsonUtil.asArray(left, right);
						if (preselect.evaluate(pair).getBooleanValue() && condition.evaluate(pair).getBooleanValue())
							this.emitCandidate(left, right);
					}
			}
		}
	}

	@Override
	protected CompositeDuplicateDetectionAlgorithm<?> getImplementation() {
		return new Blocking();
	}

	@Override
	public String toString() {
		return String.format("%s, blockingKeys=%s", super.toString(), Arrays.toString(this.blockingKeys));
	}

	/**
	 * Returns the parameter combination under test.
	 * 
	 * @return the parameter combination
	 */
	@Parameters
	public static Collection<Object[]> getParameters() {
		final EvaluationExpression[] projections = { null, getAggregativeProjection() };
		final boolean[] useIds = { /* false, */true };

		final ArrayList<Object[]> parameters = new ArrayList<Object[]>();
		for (final EvaluationExpression projection : projections)
			for (final String[][] combinedBlockingKeys : TestKeys.CombinedBlockingKeys)
				for (final boolean useId : useIds)
					parameters.add(new Object[] { projection, useId, combinedBlockingKeys });

		return parameters;
	}

}
