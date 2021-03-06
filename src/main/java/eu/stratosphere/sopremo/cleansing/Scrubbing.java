package eu.stratosphere.sopremo.cleansing;

import static eu.stratosphere.sopremo.pact.SopremoUtil.cast;

import java.io.IOException;

import eu.stratosphere.sopremo.cleansing.scrubbing.RuleBasedScrubbing;
import eu.stratosphere.sopremo.cleansing.scrubbing.ValidationRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.ValueCorrection;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.FieldAssignment;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.expressions.TernaryExpression;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.operator.SopremoModule;
import eu.stratosphere.sopremo.tree.NodeHandler;
import eu.stratosphere.sopremo.tree.ReturnlessTreeHandler;
import eu.stratosphere.sopremo.tree.TreeHandler;

@Name(verb = "scrub")
@InputCardinality(1)
@OutputCardinality(1)
public class Scrubbing extends CompositeOperator<Scrubbing> {
	private RuleBasedScrubbing ruleBasedScrubbing = new RuleBasedScrubbing();

	@Property
	@Name(preposition = "with rules")
	public void setRuleExpression(ObjectCreation ruleExpression) {
		this.ruleBasedScrubbing.clear();
		this.targetHandler.process(ruleExpression, EvaluationExpression.VALUE);
	}



	private EvaluationExpression putValueAsParameterInFunction(
			FunctionCall function) {
		FunctionCall fct = (FunctionCall) function.copy();
		fct.getParameters().add(0, EvaluationExpression.VALUE);
		return fct;
	}



	public void addRule(EvaluationExpression ruleExpression,
			PathSegmentExpression target) {
		this.ruleBasedScrubbing.addRule(ruleExpression, target);
	}

	public void removeRule(EvaluationExpression ruleExpression,
			PathSegmentExpression target) {
		this.ruleBasedScrubbing.removeRule(ruleExpression, target);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#addImplementation(
	 * eu.stratosphere.sopremo.operator.SopremoModule ,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(SopremoModule module) {
		this.ruleBasedScrubbing.addImplementation(module);
	}

	public Scrubbing withRuleExpression(ObjectCreation ruleExpression) {
		this.setRuleExpression(ruleExpression);
		return this;
	}
	
	private transient TargetHandler targetHandler = new TargetHandler();

	private class TargetHandler extends ReturnlessTreeHandler<EvaluationExpression, PathSegmentExpression> {
		
		public TargetHandler() {
			put(ObjectCreation.class, new NodeHandler<ObjectCreation, EvaluationExpression, PathSegmentExpression>() {
				@Override
				public EvaluationExpression handle(ObjectCreation value, PathSegmentExpression targetPath,
						TreeHandler<Object, EvaluationExpression, PathSegmentExpression> treeHandler) {
					for (Mapping<?> subExpr : value.getMappings()) {
						FieldAssignment assignment = cast(subExpr, ObjectCreation.FieldAssignment.class, "");
						treeHandler.handle(assignment.getExpression(),
								new ObjectAccess(assignment.getTarget()).withInputExpression(targetPath));
					}
					return null;
				}
			});
			put(ArrayCreation.class, new NodeHandler<ArrayCreation, EvaluationExpression, PathSegmentExpression>() {
				@Override
				public EvaluationExpression handle(ArrayCreation rulesArray, PathSegmentExpression targetPath,
						TreeHandler<Object, EvaluationExpression, PathSegmentExpression> treeHandler) {
					for (EvaluationExpression node : rulesArray) {
						treeHandler.handle(node, targetPath);
					}
					return null;
				}
			});
			put(TernaryExpression.class,
					new NodeHandler<TernaryExpression, EvaluationExpression, PathSegmentExpression>() {
						@Override
						public EvaluationExpression handle(
								TernaryExpression ternaryExpression,
								PathSegmentExpression targetPath,
								TreeHandler<Object, EvaluationExpression, PathSegmentExpression> treeHandler) {
							EvaluationExpression node = ternaryExpression
									.getIfClause();
							
							if(ternaryExpression.getThenExpression() instanceof ValueCorrection){
								ValueCorrection generalFix = (ValueCorrection) ((TernaryExpression) ternaryExpression)
										.getThenExpression();
								for (ValidationRule rule : node
										.findAll(ValidationRule.class)) {
									rule.setValueCorrection(generalFix);
								}
							}
							//handle left side of ?:
							treeHandler.handle(node, targetPath);
							//handle true case of ?:
							treeHandler.handle(ternaryExpression.getIfExpression(), targetPath);
							return null;
						}
					});
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.tree.TreeHandler#unknownValueType(java.lang.Object, java.lang.Object)
		 */
		@Override
		protected Object unknownValueType(EvaluationExpression value, PathSegmentExpression targetPath) {
			Scrubbing.this.sourceHandler.addRuleTo(value, targetPath);
			
			return null;
		}

		public void process(EvaluationExpression expression, PathSegmentExpression value) {
			handle(expression, value);
		}
	}

	private transient SourceHandler sourceHandler = new SourceHandler();

	private class SourceHandler extends TreeHandler<Object, EvaluationExpression, PathSegmentExpression> {
		/**
		 * Initializes EntityMapping.SourceHandler.
		 */
		public SourceHandler() {
			put(ValidationRule.class, new NodeHandler<ValidationRule, EvaluationExpression, PathSegmentExpression>() {
				@Override
				public EvaluationExpression handle(ValidationRule rule, PathSegmentExpression pathSegment,
						TreeHandler<Object, EvaluationExpression, PathSegmentExpression> treeHandler) {
						Scrubbing.this.addRule(rule.clone(), pathSegment);
						return  null;
				}
			});
			put(FunctionCall.class, new NodeHandler<FunctionCall, EvaluationExpression, PathSegmentExpression>() {
				@Override
				public EvaluationExpression handle(FunctionCall functionCall, PathSegmentExpression pathSegment,
						TreeHandler<Object, EvaluationExpression, PathSegmentExpression> treeHandler) {
						Scrubbing.this.addRule(putValueAsParameterInFunction((FunctionCall)functionCall.clone()), pathSegment);
						return  null;
				}
			});
		}
		
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.tree.TreeHandler#unknownValueType(java.lang.Object, java.lang.Object)
		 */
		@Override
		protected EvaluationExpression unknownValueType(Object value, PathSegmentExpression targetPath) {
			if(ValidationRule.class.isAssignableFrom(value.getClass())){
				NodeHandler<Object, EvaluationExpression, PathSegmentExpression> nodeHandler = get(ValidationRule.class);
				if (nodeHandler == null)
					throw new IllegalStateException("this schould not happen");
				return nodeHandler.handle(value, targetPath, this);
			}
			return null;
		}

		public void addRuleTo(EvaluationExpression cleansingRule, PathSegmentExpression targetPath) {
			handle(cleansingRule, targetPath);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.ruleBasedScrubbing.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		Scrubbing other = (Scrubbing) obj;
		return this.ruleBasedScrubbing.equals(other.ruleBasedScrubbing);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.operator.ElementaryOperator#appendAsString(java
	 * .lang.Appendable)
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		this.ruleBasedScrubbing.appendAsString(appendable);
	}
}
