package eu.stratosphere.sopremo.cleansing.mapping;

import it.unibas.spicy.model.datasource.INode;
import it.unibas.spicy.model.datasource.nodes.LeafNode;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.FunctionCall;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.NestedOperatorExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ObjectCreation.Mapping;
import eu.stratosphere.sopremo.expressions.TernaryExpression;
import eu.stratosphere.sopremo.expressions.tree.ChildIterator;
import eu.stratosphere.sopremo.operator.CompositeOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.OutputCardinality;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.operator.SopremoModule;

/**
 * @author Arvid Heise, Fabian Tschirschnitz, Tommy Neubert
 */
@Name(noun = "map entities of")
@InputCardinality(min = 1)
@OutputCardinality(min = 1)
public class EntityMapping extends CompositeOperator<EntityMapping> {

	protected static final String type = "XML";

	protected static final String targetStr = "target";

	protected static final String sourceStr = "source";

	protected static final String entitiesStr = "entities_";

	protected static final String entityStr = "entity_";

	protected static final String idStr = "id";
	
	protected static final String separator = ".";

	protected static INode dummy = new LeafNode("dummy");

	private SpicyMappingTransformation spicyMappingTransformation = new SpicyMappingTransformation();

	private HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeysPaths = new HashMap<SpicyPathExpression, SpicyPathExpression>();

	private ArrayCreation mappingExpression;

	private boolean schemasInitialized = false;

	public SpicyMappingTransformation getSpicyMappingTransformation() {
		return spicyMappingTransformation;
	}

	public void setSpicyMappingTransformation(SpicyMappingTransformation spicyMappingTransformation) {
		this.spicyMappingTransformation = spicyMappingTransformation;
	}

	//defines the relations between the source entities
	@Property
	@Name(preposition = "where")
	public void setForeignKeys(final BooleanExpression assignment) {
		//init schemas
		this.createDefaultSourceSchema(this.getInputs().size());
		this.createDefaultTargetSchema(this.getNumOutputs());
		this.schemasInitialized  = true;
		
		//single join
		if (assignment instanceof ComparativeExpression) {
			handleSourceJoinExpression((ComparativeExpression) assignment);
		//multiple join
		} else {
			for (final ChildIterator it = assignment.iterator(); it.hasNext();) {
				final EvaluationExpression expr = it.next();
				final ComparativeExpression condidition = (ComparativeExpression) expr;
				handleSourceJoinExpression((ComparativeExpression) condidition);
			}
		}
		
		
	}
	
	// creates a join condition between two input sources
	private void handleSourceJoinExpression(ComparativeExpression condidition) {
		final ObjectAccess left = condidition.getExpr1().findFirst(
				ObjectAccess.class);
		final ObjectAccess right = condidition.getExpr2().findFirst(
				ObjectAccess.class);
		final InputSelection leftInput = left.findFirst(InputSelection.class);
		final InputSelection rightInput = right.findFirst(InputSelection.class);
		final String leftJoinAttribute = left.getField();
		final String rightJoinAttribute = right.getField();

		final String sourceNesting = this.createNesting(
				EntityMapping.sourceStr, leftInput.getIndex());
		final String targetNesting = this.createNesting(
				EntityMapping.sourceStr, rightInput.getIndex());

		MappingJoinCondition joinCondition = this.createJoinCondition(
				sourceNesting, leftJoinAttribute, targetNesting,
				rightJoinAttribute);

		this.spicyMappingTransformation.getMappingInformation()
				.getSourceJoinConditions().add(joinCondition);

		// extend source schema by join attributes
		this.extendSourceSchemaBy(rightJoinAttribute, rightInput.getIndex());
		this.extendSourceSchemaBy(leftJoinAttribute, leftInput.getIndex());
	}

	public EntityMapping withForeignKeys(final BooleanExpression assignment) {
		setForeignKeys(assignment);
		return this;
	}

	public EntityMapping withMappingExpression(final ArrayCreation assignment) {
		setMappingExpression(assignment);
		return this;
	}

	/**
	 * Mapping Task: value correspondences: mappings and grouping keys source
	 * and target schema: attributes in mappings and grouping keys target keys:
	 * grouping keys join conditions with foreign keys: where-clause and foreign
	 * keys
	 */
	@Property
	@Name(adjective = "into")
	public void setMappingExpression(final ArrayCreation assignment) {
		//init schemas
		if(!this.schemasInitialized){
			this.createDefaultSourceSchema(this.getInputs().size());
			this.createDefaultTargetSchema(this.getNumOutputs());
			this.schemasInitialized  = true;
		}
				
		// iterate over schema mapping groupings
		final MappingInformation mappingInformation = this.spicyMappingTransformation.getMappingInformation();
		for (int index = 0; index < assignment.size(); index++) { // operator
																	// level

			final NestedOperatorExpression nestedOperator = (NestedOperatorExpression) assignment.get(index);
			final IdentifyOperator operator = (IdentifyOperator) nestedOperator.getOperator();
			EvaluationExpression groupingKey = operator.getKeyExpression();
			final Integer targetInputIndex = index; 
			final InputSelection sourceInput = groupingKey.findFirst(InputSelection.class); 
			String sourceNesting = this.createNesting(EntityMapping.sourceStr, sourceInput.getIndex()); // e.g.
			// source.entities_in0.entity_in0
			final String targetNesting = this.createNesting(EntityMapping.targetStr, targetInputIndex); // e.g.
			// target.entities_in0.entity_in0

			// add target entity id to target schema
			this.extendTargetSchemaBy(EntityMapping.idStr, targetInputIndex);

			// create primary key: target entity id
			MappingKeyConstraint targetKey = new MappingKeyConstraint(targetNesting, EntityMapping.idStr);
			mappingInformation.getTarget().addKeyConstraint(targetKey);
			
			if(groupingKey instanceof ObjectAccess){
				final String keyStr = ((ObjectAccess) groupingKey).getField();
				// add source grouping key to source schema
				this.extendSourceSchemaBy(keyStr, sourceInput.getIndex());
				// create value correspondence: source grouping key -> target entity id
				MappingValueCorrespondence corr = this.createValueCorrespondence(sourceNesting, keyStr, targetNesting, EntityMapping.idStr);
				mappingInformation.getValueCorrespondences().add(corr);
			} else {
				throw new IllegalArgumentException("Not implemented yet...");
			}
			ObjectCreation resultProjecition = (ObjectCreation) operator.getResultProjection();
			handleObjectCreation(resultProjecition, targetNesting, "",targetInputIndex);
			
		}
		
		// create transitive value correspondences from foreign keys
		Set<MappingValueCorrespondence> transitiveValueCorrespondences = createTransitiveValueCorrespondences();

		for (MappingValueCorrespondence cond : transitiveValueCorrespondences) {
			mappingInformation.getValueCorrespondences().add(cond);
		}
	}
	
	private void handleObjectCreation(ObjectCreation objectCreation, String targetNesting, String targetAttributeNesting, Integer targetInputIndex){
		
		final List<Mapping<?>> mappings = objectCreation.getMappings();
		//iterate over all the mappings that we got
		for (final Mapping<?> mapping : mappings) { // mapping level

			final EvaluationExpression expr = mapping.getExpression();

			if (expr instanceof FunctionCall || expr instanceof ArrayAccess || expr instanceof TernaryExpression
					|| expr instanceof ArrayCreation
					|| expr instanceof ConstantExpression) {
				//handleSpecialExpression(foreignKeys, mappingInformation, targetInputIndex, targetNesting, mapping,
					//	expr);
			} else if (expr instanceof ObjectAccess) {
				// add target attribute to target schema
				extendTargetSchema(mapping, targetAttributeNesting, targetInputIndex);
				handleObjectAccess(targetInputIndex, targetNesting, mapping);
			} else if (expr instanceof ObjectCreation) {
				String tempTargetNesting= targetNesting+EntityMapping.separator+mapping.getTarget().toString();
				String tempTargetAttributeNesting= targetAttributeNesting+(targetAttributeNesting.isEmpty()?"":EntityMapping.separator)+mapping.getTarget().toString();
				handleObjectCreation((ObjectCreation) expr, tempTargetNesting, tempTargetAttributeNesting, targetInputIndex);
			}else if (expr instanceof AggregationExpression) {
				//handleTakeAll(foreignKeys, mappingInformation, targetInputIndex, targetNesting, mapping,
					//	(AggregationExpression) expr);
			} else {
				throw new IllegalArgumentException("No valid value correspondence was given: " + expr);
			}
		}
	}

	/*private void handleTakeAll(HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys2, MappingInformation mappingInformation,
			Integer targetInputIndex, String targetNesting, Mapping<?> mapping, AggregationExpression aggregationExpression) {
		MappingValueCorrespondence corr;
		
		EvaluationExpression expr = ((ArrayProjection)aggregationExpression.getInputExpression()).getInputExpression();
		
		if (expr instanceof FunctionCall || expr instanceof ArrayAccess || expr instanceof TernaryExpression
				|| expr instanceof ObjectCreation || expr instanceof ArrayCreation
				|| expr instanceof ConstantExpression) {
			corr = handleSpecialExpression(foreignKeys2, mappingInformation, targetInputIndex, targetNesting, mapping,
					expr);
		} else if (expr instanceof ObjectAccess) {
			corr = handleObjectAccess(foreignKeys2, mappingInformation, targetInputIndex, targetNesting, mapping,
					(ObjectAccess) expr, false);
		}  else {
			throw new IllegalArgumentException("No valid value correspondence was given: " + expr);
		}
		corr.setTakeAllValuesOfGrouping(true);
	}*/
	
	private void extendTargetSchema(final Mapping<?> mapping, String targetAttributeNesting,
			Integer targetInputIndex) {
		// the attribute we map to
		final String targetAttribute = mapping.getTarget().toString();
		this.extendTargetSchemaBy((targetAttributeNesting.isEmpty()?"":targetAttributeNesting+EntityMapping.separator)+targetAttribute, targetInputIndex);
	}
	
	private void handleObjectAccess(final Integer targetInputIndex, final String targetNesting, final Mapping<?> mapping) {
		MappingValueCorrespondence corr = null;
		String sourceNesting;

		final ObjectAccess expr = (ObjectAccess)mapping.getExpression();
		final EvaluationExpression sourceInputExpr = expr.getInputExpression();
		
		//the attribute we map from
		final String sourceAttribute = expr.getField();

		//the attribute we map to
		final String targetAttribute = mapping.getTarget().toString();
		
		//determine sourceIndex
		final Integer sourceIndex;
		if(sourceInputExpr.findFirst(InputSelection.class)!=null){
			sourceIndex = sourceInputExpr.findFirst(InputSelection.class).getIndex();
		}else{
			sourceIndex =  Integer.parseInt(sourceInputExpr.toString().replaceAll("[^0-9]", ""));
		}
		
		sourceNesting = this.createNesting(EntityMapping.sourceStr, sourceIndex);

		//handle foreign key reference
		if (sourceInputExpr.toString().contains(this.getClass().getSimpleName())) {
			
			sourceNesting = this.createNesting(EntityMapping.targetStr, sourceIndex);

			// create join condition for foreign keys, but no value correspondence
			MappingJoinCondition targetJoinCondition = this.createJoinCondition(targetNesting, targetAttribute, sourceNesting, sourceAttribute);
			this.spicyMappingTransformation.getMappingInformation().getTargetJoinConditions().add(targetJoinCondition);

			// store foreign keys to add missing (transitive) value correspondences later
			foreignKeysPaths.put(targetJoinCondition.getFromPaths().get(0), targetJoinCondition.getToPaths().get(0));
		} else { // no foreign key

			// add source attribute to source schema
			this.extendSourceSchemaBy(sourceAttribute, sourceInputExpr.findFirst(InputSelection.class).getIndex());

			// create value correspondence: source attribute -> target attribute
			corr = this.createValueCorrespondence(sourceNesting, sourceAttribute,
					targetNesting, targetAttribute);
			this.spicyMappingTransformation.getMappingInformation().getValueCorrespondences().add(corr);
		}
	}

	/*private MappingValueCorrespondence handleSpecialExpression(HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys, final MappingInformation mappingInformation,
			final Integer targetInputIndex, final String targetNesting, final Mapping<?> mapping, EvaluationExpression expr) {
		List<SpicyPathExpression> sourcePaths = new ArrayList<SpicyPathExpression>();
		for (ObjectAccess oa : expr.findAll(ObjectAccess.class)) {
			handleObjectAccessInSpecialExpression(foreignKeys, targetInputIndex, targetNesting, mappingInformation, mapping, oa, sourcePaths);
		}
		this.extendTargetSchemaBy(mapping.getTarget().toString(), targetInputIndex);
		MappingValueCorrespondence corr = this.createValueCorrespondence(sourcePaths, targetNesting, mapping.getTarget().toString(), expr);
		mappingInformation.getValueCorrespondences().add(corr);
		return corr;
	}*/
	
	/*private void handleObjectAccessInSpecialExpression(HashMap<SpicyPathExpression, SpicyPathExpression> foreignKeys, Integer targetInputIndex, String targetNesting, MappingInformation mappingInformation, Mapping<?> mapping, ObjectAccess oa, List<SpicyPathExpression> sourcePaths) {
		String sourceNesting;
		handleObjectAccess(foreignKeys, mappingInformation, targetInputIndex, targetNesting, mapping, oa, true);
		
		final EvaluationExpression sourceInputExpr = oa.getInputExpression();
		final String sourceExpr = oa.getField();

		//TODO this does not work as expected
		if (sourceInputExpr.toString().contains(this.getClass().getSimpleName())) {
			final Integer fkSource =  Integer.parseInt(sourceInputExpr.toString().replaceAll("[^0-9]", ""));
			sourceNesting = this.createNesting(EntityMapping2.targetStr, fkSource);
			MappingJoinCondition targetJoinCondition;

			final String targetExpr = mapping.getTarget().toString();
			targetJoinCondition = this.createJoinCondition(targetNesting, targetExpr, sourceNesting, sourceExpr);
			mappingInformation.getTargetJoinConditions().add(targetJoinCondition);

			foreignKeys.put(targetJoinCondition.getFromPaths().get(0), targetJoinCondition.getToPaths().get(0));
			
		}else{
			sourceNesting = this.createNesting(EntityMapping2.sourceStr, sourceInputExpr.findFirst(InputSelection.class).getIndex());
			sourcePaths.add(new SpicyPathExpression(sourceNesting, sourceExpr));
		}
	}*/

	/**
	 * Returns the mappingExpression.
	 * 
	 * @return the mappingExpression
	 */
	public ArrayCreation getMappingExpression() {
		return this.mappingExpression;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.spicyMappingTransformation.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		EntityMapping other = (EntityMapping) obj;
		return this.spicyMappingTransformation.equals(other.spicyMappingTransformation);
	}

	private void createDefaultSourceSchema(final int size) {
		this.spicyMappingTransformation.getMappingInformation().setSourceSchema(new MappingSchema(size, EntityMapping.sourceStr));
	}

	private void createDefaultTargetSchema(final int size) {
		this.spicyMappingTransformation.getMappingInformation().getTarget().setTargetSchema(new MappingSchema(size, EntityMapping.targetStr));

	}

	private void extendSourceSchemaBy(final String attr, final Integer inputIndex) {
		this.spicyMappingTransformation.getMappingInformation().getSourceSchema().addKeyToInput(inputIndex, attr);
	}

	private void extendTargetSchemaBy(final String attr, final Integer inputStr) {
		this.spicyMappingTransformation.getMappingInformation().getTarget().getTargetSchema().addKeyToInput(inputStr, attr);
	}

	private String createNesting(final String type, final Integer inputIndex) {

		final StringBuilder builder = new StringBuilder().append(type).append(separator).append(EntityMapping.entitiesStr).append(inputIndex)
				.append(separator).append(EntityMapping.entityStr).append(inputIndex);

		return builder.toString();
	}

	private MappingValueCorrespondence createValueCorrespondence(final String sourceNesting, final String sourceAttr, final String targetNesting,
			final String targetAttr) {
		final SpicyPathExpression sourceSteps = new SpicyPathExpression(sourceNesting, sourceAttr);
		final SpicyPathExpression targetSteps = new SpicyPathExpression(targetNesting, targetAttr);

		return new MappingValueCorrespondence(sourceSteps, targetSteps);
	}
	
	private MappingValueCorrespondence createValueCorrespondence(final List<SpicyPathExpression> sourcePaths, final String targetNesting,
			final String targetAttr, EvaluationExpression expr) {
		final SpicyPathExpression targetSteps = new SpicyPathExpression(targetNesting, targetAttr);

		return new MappingValueCorrespondence(sourcePaths, targetSteps, expr);
	}

	private MappingJoinCondition createJoinCondition(final String sourceNesting, final String sourceAttr, final String targetNesting, final String targetAttr) {
		MappingJoinCondition joinCondition = new MappingJoinCondition(Collections.singletonList(new SpicyPathExpression(sourceNesting, sourceAttr)),
				Collections.singletonList(new SpicyPathExpression(targetNesting, targetAttr)), true, true);

		return joinCondition;
	}

	private Set<MappingValueCorrespondence> createTransitiveValueCorrespondences() {

		Set<MappingValueCorrespondence> transitiveValueCorrespondences = new HashSet<MappingValueCorrespondence>();
		for (SpicyPathExpression fk : this.foreignKeysPaths.keySet()) {
			SpicyPathExpression value = this.foreignKeysPaths.get(fk);

			for (MappingValueCorrespondence mvc : this.spicyMappingTransformation.getMappingInformation().getValueCorrespondences()) {
				if (mvc.getTargetPath().equals(value)) {
					MappingValueCorrespondence correspondence = new MappingValueCorrespondence(mvc.getSourcePaths().get(0), fk);
					transitiveValueCorrespondences.add(correspondence);
				}
			}
		}

		return transitiveValueCorrespondences;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.operator.Operator#appendAsString(java.lang.Appendable
	 * )
	 */
	@Override
	public void appendAsString(Appendable appendable) throws IOException {
		super.appendAsString(appendable);
		this.spicyMappingTransformation.getMappingInformation().appendAsString(appendable);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * eu.stratosphere.sopremo.operator.CompositeOperator#addImplementation(
	 * eu.stratosphere.sopremo.operator. SopremoModule,
	 * eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public void addImplementation(final SopremoModule module) {
		Map<String, Integer> inputIndex = new HashMap<String, Integer>();
		for (int i = 0; i < this.getNumInputs(); i++) {
			inputIndex.put(entitiesStr + i, i);
		}
		Map<String, Integer> outputIndex = new HashMap<String, Integer>();
		// FIXME hack to solve #output problem
		this.getOutputs();

		for (int i = 0; i < this.getNumOutputs(); i++) {
			outputIndex.put(entitiesStr + i, i);
		}
		this.spicyMappingTransformation.setInputIndex(inputIndex);
		this.spicyMappingTransformation.setOutputIndex(outputIndex);
		this.spicyMappingTransformation.addImplementation(module);
	}
}