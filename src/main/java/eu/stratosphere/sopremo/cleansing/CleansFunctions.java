package eu.stratosphere.sopremo.cleansing;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.util.regex.Pattern;

import eu.stratosphere.sopremo.cache.NodeCache;
import eu.stratosphere.sopremo.cleansing.scrubbing.DefaultValueCorrection;
import eu.stratosphere.sopremo.cleansing.scrubbing.NonNullRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.PatternValidationRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.RangeRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.UnresolvableCorrection;
import eu.stratosphere.sopremo.cleansing.scrubbing.ValidationRule;
import eu.stratosphere.sopremo.cleansing.scrubbing.ValueCorrection;
import eu.stratosphere.sopremo.cleansing.similarity.Similarity;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityExpression;
import eu.stratosphere.sopremo.cleansing.similarity.SimilarityFactory;
import eu.stratosphere.sopremo.cleansing.similarity.set.JaccardSimilarity;
import eu.stratosphere.sopremo.cleansing.similarity.text.JaroWinklerSimilarity;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.PathSegmentExpression;
import eu.stratosphere.sopremo.function.MacroBase;
import eu.stratosphere.sopremo.function.SopremoFunction;
import eu.stratosphere.sopremo.function.SopremoFunction1;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.packages.BuiltinProvider;
import eu.stratosphere.sopremo.packages.ConstantRegistryCallback;
import eu.stratosphere.sopremo.packages.FunctionRegistryCallback;
import eu.stratosphere.sopremo.packages.IConstantRegistry;
import eu.stratosphere.sopremo.packages.IFunctionRegistry;
import eu.stratosphere.sopremo.type.CachingArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.TextNode;
import eu.stratosphere.sopremo.type.TypeCoercer;
//0.2compability
//import eu.stratosphere.sopremo.SopremoEnvironment;

public class CleansFunctions implements BuiltinProvider, ConstantRegistryCallback, FunctionRegistryCallback {

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.packages.ConstantRegistryCallback#registerConstants
	 * (eu.stratosphere.sopremo.packages. IConstantRegistry)
	 */
	@Override
	public void registerConstants(IConstantRegistry constantRegistry) {
		constantRegistry.put("required", new NonNullRule());
		constantRegistry.put("chooseNearestBound", CHOOSE_NEAREST_BOUND);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.packages.FunctionRegistryCallback#registerFunctions
	 * (eu.stratosphere.sopremo.packages. IFunctionRegistry)
	 */
	@Override
	public void registerFunctions(IFunctionRegistry registry) {
		registry.put("jaccard", new SimilarityMacro(new JaccardSimilarity()));
		registry.put("jaroWinkler", new SimilarityMacro(new JaroWinklerSimilarity()));
		registry.put("patternValidation", new PatternValidationRuleMacro());
		registry.put("range", new RangeRuleMacro());
		registry.put("default", new DefaultValueCorrectionMacro());
		
		// 0.2compability
		// registry.put("vote", new VoteMacro());
	}

	public static final SopremoFunction GENERATE_ID = new GenerateId();

	@Name(verb = "generateId")
	public static class GenerateId extends SopremoFunction1<TextNode> {
		GenerateId() {
			super("generateId");
		}

		private transient TextNode resultId = new TextNode();

		private transient int id = 0;

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.cleansing.CleansFunctions.LONGEST#call(eu
		 * .stratosphere.sopremo.type.IArrayNode)
		 */
		@Override
		public IJsonNode call(TextNode prefix) {
			this.resultId.clear();
			this.resultId.append(prefix);
			this.resultId.append(this.id++);
			return this.resultId;
		}
	};

	public static final SopremoFunction SOUNDEX = new SoundEx();

	@Name(noun = "soundex")
	public static class SoundEx extends SopremoFunction1<TextNode> {
		SoundEx() {
			super("soundex");
		}

		private transient TextNode soundex = new TextNode();

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.cleansing.CleansFunctions.LONGEST#call(eu
		 * .stratosphere.sopremo.type.IArrayNode)
		 */
		@Override
		public IJsonNode call(TextNode input) {
			this.soundex.clear();
			try {
				eu.stratosphere.sopremo.cleansing.blocking.SoundEx.generateSoundExInto(input, this.soundex);
			} catch (IOException e) {
			}
			return this.soundex;
		}
	};

	// 0.2compability
	// @Name(verb = "removeVowels")
	// public static final SopremoFunction REMOVE_VOWELS =
	// CoreFunctions.REPLACE.bind(TextNode.valueOf("(?i)[aeiou]"),
	// TextNode.valueOf(""));

	public static final SopremoFunction LONGEST = new Longest();

	@Name(adjective = "longest")
	public static class Longest extends SopremoFunction1<IArrayNode<IJsonNode>> {
		Longest() {
			super("longest");
		}

		private transient NodeCache nodeCache = new NodeCache();

		private transient CachingArrayNode<IJsonNode> result = new CachingArrayNode<IJsonNode>();

		private transient IntList sizes = new IntArrayList();

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.cleansing.CleansFunctions.LONGEST#call(eu
		 * .stratosphere.sopremo.type.IArrayNode)
		 */
		@Override
		public IJsonNode call(IArrayNode<IJsonNode> values) {
			this.result.setSize(0);
			if (values.isEmpty())
				return this.result;

			if (values.size() == 1) {
				this.result.addClone(values.get(0));
				return this.result;
			}

			this.sizes.clear();
			for (IJsonNode value : values)
				this.sizes.add(TypeCoercer.INSTANCE.coerce(value, this.nodeCache, TextNode.class).length());
			int maxSize = this.sizes.getInt(0);
			for (int index = 1; index < this.sizes.size(); index++)
				maxSize = Math.max(index, maxSize);
			for (int index = 0; index < this.sizes.size(); index++)
				if (maxSize == this.sizes.getInt(index))
					this.result.addClone(values.get(index));
			return this.result;
		}
	};

	private static class PatternValidationRuleMacro extends MacroBase {

		/*
		 * (non-Javadoc)
		 * 
		 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object)
		 */
		@Override
		public PatternValidationRule call(EvaluationExpression[] params) {

			if (params.length == 1)
				return new PatternValidationRule(Pattern.compile(params[0].evaluate(NullNode.getInstance()).toString()));
			else
				throw new IllegalArgumentException("Wrong number of arguments.");

		}

		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			// TODO Auto-generated method stub

		}
	}

	private static class RangeRuleMacro extends MacroBase {

		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public EvaluationExpression call(EvaluationExpression[] params) {
			if (params.length == 2) {
				return new RangeRule(params[0].evaluate(NullNode.getInstance()), params[1].evaluate(NullNode.getInstance()));
			} else {
				throw new IllegalArgumentException("Wrong number of arguments.");
			}
		}

	}
	
	private static class DefaultValueCorrectionMacro extends MacroBase {

		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			// TODO Auto-generated method stub
		}

		@Override
		public EvaluationExpression call(EvaluationExpression[] params) {
			if (params.length == 1) {
				return new DefaultValueCorrection(params[0].evaluate(NullNode.getInstance()));
			} else {
				throw new IllegalArgumentException("Wrong number of arguments.");
			}
		}

	}

	/**
	 * @author Arvid Heise
	 */
	private static class SimilarityMacro extends MacroBase {
		private final Similarity<?> similarity;

		/**
		 * Initializes SimmetricMacro.
		 * 
		 * @param similarity
		 */
		public SimilarityMacro(Similarity<?> similarity) {
			this.similarity = similarity;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable
		 * )
		 */
		@Override
		public void appendAsString(Appendable appendable) throws IOException {
			this.similarity.appendAsString(appendable);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object)
		 */
		@SuppressWarnings("unchecked")
		@Override
		public EvaluationExpression call(EvaluationExpression[] params) {
			for (EvaluationExpression evaluationExpression : params)
				if (!(evaluationExpression instanceof PathSegmentExpression))
					throw new IllegalArgumentException("Can only expand simple path expressions");

			Similarity<IJsonNode> similarity;
			if (params.length > 1)
				similarity = (Similarity<IJsonNode>) SimilarityFactory.INSTANCE.create(this.similarity, (PathSegmentExpression) params[0],
						(PathSegmentExpression) params[1], true);
			else
				similarity = (Similarity<IJsonNode>) SimilarityFactory.INSTANCE.create(this.similarity, (PathSegmentExpression) params[0],
						(PathSegmentExpression) params[0], true);
			return new SimilarityExpression(similarity);
		}
	}

	private static final ValueCorrection CHOOSE_NEAREST_BOUND = new ValueCorrection() {
		private Object readResolve() {
			return CHOOSE_NEAREST_BOUND;
		}

		@Override
		public IJsonNode fix(IJsonNode value, ValidationRule violatedRule) {
			if (violatedRule instanceof RangeRule) {
				final RangeRule that = (RangeRule) violatedRule;
				if (that.getMin().compareTo(value) > 0)
					return that.getMin();
				return that.getMax();
			} else {
				return UnresolvableCorrection.INSTANCE.fix(value, violatedRule);
			}
		}
	};
	
	// 0.2compability
	// private static class VoteMacro extends MacroBase {
	// /**
	// * Initializes CleansFunctions.VoteMacro.
	// */
	// public VoteMacro() {
	// }
	//
	// /*
	// * (non-Javadoc)
	// * @see eu.stratosphere.sopremo.function.Callable#call(java.lang.Object,
	// java.lang.Object,
	// * eu.stratosphere.sopremo.EvaluationContext)
	// */
	// @Override
	// public EvaluationExpression call(EvaluationExpression[] params) {
	// for (int index = 0; index < params.length; index++)
	// if (params[index] instanceof MethodPointerExpression) {
	// final String functionName = ((MethodPointerExpression)
	// params[index]).getFunctionName();
	// params[index] = new FunctionCall(functionName,
	// SopremoEnvironment.getInstance()
	// .getEvaluationContext(), EvaluationExpression.VALUE);
	// }
	// return new BeliefResolution(params);
	// }
	//
	// /*
	// * (non-Javadoc)
	// * @see
	// eu.stratosphere.sopremo.ISopremoType#appendAsString(java.lang.Appendable)
	// */
	// @Override
	// public void appendAsString(Appendable appendable) throws IOException {
	// appendable.append("vote");
	// }
	//
	// }

	//
	// @Name(verb = "satisfies")
	// public static class Filter extends
	// SopremoFunction2<IArrayNode<IJsonNode>, FunctionNode> {
	// Filter() {
	// super("satisfies");
	// }
	//
	// @Override
	// protected IJsonNode call(IArrayNode<IJsonNode> input, final FunctionNode
	// mapExpression) {
	// SopremoUtil.assertArguments(mapExpression.getFunction(), 1);
	//
	// this.result.clear();
	// final FunctionCache calls = this.caches.get(mapExpression.getFunction());
	// for (int index = 0; index < input.size(); index++) {
	// this.parameters.set(0, input.get(index));
	// this.result.add(calls.get(index).call(this.parameters));
	// }
	// return this.result;
	// }
	// };
}