package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.List;

import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * This class provides the functionality to specify a certain number of values
 * that are allowed as values for a specific record-field. Only values that are
 * included in this, so called 'white list' are allowed in the resulting
 * records. The following example shows the usage of this rule in a meteor-script:
 * 
 * * <code><pre>
 * ...
 * $persons_scrubbed = scrub $persons_sample with rules {
 *	...
 *	person_type: containedIn(["customer", "employee", "founder"]),
 *	...
 * };
 * ...
 * </pre></code>
 * 
 * @author Arvid Heise, Tommy Neubert, Fabian Tschirschnitz
 */
public class WhiteListRule extends ValidationRule {
	private final List<IJsonNode> possibleValues;

	@SuppressWarnings("unchecked")
	public WhiteListRule(List<? extends IJsonNode> possibleValues) {
		this.possibleValues = (List<IJsonNode>) possibleValues;
	}

	@SuppressWarnings("unchecked")
	public WhiteListRule(List<? extends IJsonNode> possibleValues,
			IJsonNode defaultValue) {
		this.possibleValues = (List<IJsonNode>) possibleValues;
		this.setValueCorrection(new DefaultValueCorrection(defaultValue));
	}

	/**
	 * Initializes WhiteListRule.
	 * 
	 */
	WhiteListRule() {
		this.possibleValues = null;
	}

	@Override
	public boolean validate(IJsonNode value) {
		return this.possibleValues.contains(value);
	}

	public List<IJsonNode> getPossibleValues() {
		return this.possibleValues;
	}
}
