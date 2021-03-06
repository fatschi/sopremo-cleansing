package eu.stratosphere.sopremo.cleansing.scrubbing;

import eu.stratosphere.sopremo.cleansing.CleansFunctions;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;

/**
 * This class provides the functionality to specify the allowed range of values
 * for a record-field. The evaluation of this rule is symmetric to the
 * compareTo-method. Both, the lower and upper bound, are treated ase the
 * lowest/highest allowed value for the field. The following example shows the
 * usage of this rule in a meteor-script: <code><pre>
 * ...
 * $persons_scrubbed = scrub $persons_sample with rules {
 * 	...
 * 	age: range(0, 100),
 *  code: range("A", "Z"),
 * 	...
 * };
 * ...
 * </pre></code> implemented corrections: <br/>
 * - {@link CleansFunctions#CHOOSE_NEAREST_BOUND}
 * 
 * @author Arvid Heise, Tommy Neubert, Fabian Tschirschnitz
 */
@Name(adjective = "range")
public class RangeConstraint extends ValidationRule {
	private IJsonNode min, max;

	public RangeConstraint(final IJsonNode min, final IJsonNode max) {
		this.min = min;
		this.max = max;
		setValueCorrection(CleansFunctions.CHOOSE_NEAREST_BOUND);
	}

	/**
	 * Initializes RangeRule.
	 */
	RangeConstraint() {
		this(IntNode.ZERO, IntNode.ONE);
	}

	@Override
	public boolean validate(final IJsonNode value) {
		return this.min.compareTo(value) <= 0 && value.compareTo(this.max) <= 0;
	}

	public IJsonNode getMin() {
		return min;
	}

	public IJsonNode getMax() {
		return max;
	}
}
