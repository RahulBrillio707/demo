"""
LLM-based Dataset Lineage Generator for SAS Code

This module uses LLM to generate comprehensive dataset lineage from extracted AST context.
"""

import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enhanced_ast_walker import LineageExtractionContext
from langchain_openai import AzureChatOpenAI


@dataclass
class LineageNode:
    """A node in the dataset lineage graph"""
    dataset_name: str
    dataset_type: str  # 'source', 'intermediate', 'final'
    created_by: str  # Operation that created this dataset
    inputs: List[str]  # Input datasets
    transformations: List[str]  # Transformation descriptions
    variables_created: List[str]  # New variables created
    business_logic: str  # Natural language description


@dataclass
class LineageRelationship:
    """Relationship between datasets"""
    source_dataset: str
    target_dataset: str
    transformation_type: str  # 'data_step', 'sql_join', 'aggregation'
    transformation_description: str
    variables_passed: List[str]  # Variables that flow from source to target


@dataclass
class DatasetLineage:
    """Complete dataset lineage"""
    nodes: List[LineageNode]
    relationships: List[LineageRelationship]
    flow_description: str  # Overall data flow narrative
    variable_lineage: Dict[str, List[str]]  # variable -> source_variables


class LLMLineageGenerator:
    """Generate dataset lineage using LLM"""
    
    def __init__(self, llm_client: Optional[AzureChatOpenAI] = None):
        self.llm_client = llm_client or self._get_llm_client()
    
    def _get_llm_client(self) -> AzureChatOpenAI:
        """Get configured Azure OpenAI LLM client"""
        return AzureChatOpenAI(
        azure_deployment="gpt-5-chat-tesco",
        model_name="gpt-5-chat",
        api_version="2024-12-01-preview",
        azure_endpoint="https://swapn-mfb5frew-eastus2.cognitiveservices.azure.com/",
        temperature=0,
        max_tokens=None,
        timeout=None,
        max_retries=2,
        api_key="2TQatEAtwPIr2fIyVHgyqr7fwmgT3Hu5TClSIGvtTkPp3ydd3t71JQQJ99BIACHYHv6XJ3w3AAAAACOGiqNo"
    )
    
    def generate_lineage(self, context: LineageExtractionContext) -> DatasetLineage:
        """Generate comprehensive dataset lineage from context"""
        
        # Prepare context for LLM
        llm_context = self._prepare_llm_context(context)
        
        # Generate lineage using LLM
        prompt = self._build_lineage_prompt(llm_context)
        
        # Call Azure OpenAI LLM API
        try:
            response = self.llm_client.invoke(prompt)
            llm_response = response.content
            return self._parse_llm_response(llm_context, llm_response)
        except Exception as e:
            print(f"Warning: LLM API call failed: {e}")
            print("Falling back to sample response...")
            return self._parse_llm_response(llm_context, "")
    
    def _prepare_llm_context(self, context: LineageExtractionContext) -> Dict[str, Any]:
        """Prepare comprehensive context for LLM"""
        
        return {
            "operation_summary": {
                "total_steps": len(context.data_steps) + len(context.sql_operations),
                "data_steps": len(context.data_steps),
                "sql_operations": len(context.sql_operations),
                "sequence": context.operation_sequence
            },
            
            "datasets": context.dataset_summary,
            
            "data_transformations": [
                {
                    "step_id": ds.step_id,
                    "step_name": ds.step_name,
                    "operation_type": "data_step",
                    "inputs": ds.input_datasets,
                    "outputs": ds.output_datasets,
                    "sequence_order": ds.sequence_order,
                    "transformations": [
                        {
                            "type": t.transformation_type,
                            "description": t.description,
                            "variables_involved": t.variables_involved,
                            "logic": t.logic
                        } for t in ds.transformations
                    ],
                    "variable_derivations": ds.variable_derivations,
                    "filtering_logic": ds.filtering_logic,
                    "conditional_logic": ds.conditional_logic
                } for ds in context.data_steps
            ],
            
            "sql_transformations": [
                {
                    "step_id": sql.step_id,
                    "output_table": sql.output_table,
                    "operation_type": "proc_sql",
                    "inputs": sql.input_tables,
                    "outputs": [sql.output_table],
                    "sequence_order": sql.sequence_order,
                    "select_columns": sql.select_columns,
                    "join_logic": sql.join_logic,
                    "where_conditions": sql.where_conditions,
                    "group_by_columns": sql.group_by_columns,
                    "calculated_fields": sql.calculated_fields,
                    "aggregations": sql.aggregations
                } for sql in context.sql_operations
            ],
            
            "implicit_relationships": context.implicit_relationships
        }
    
    def _build_lineage_prompt(self, llm_context: Dict[str, Any]) -> str:
        """Build comprehensive prompt for LLM lineage generation"""
        
        prompt = f"""# SAS Dataset Lineage Analysis Task

You are an expert data engineer analyzing SAS code to create comprehensive dataset lineage. 

## Context Information:
{json.dumps(llm_context, indent=2)}

## Your Task:
Create a comprehensive dataset lineage that shows:

1. **Dataset Flow**: How data flows from source datasets through transformations to final outputs
2. **Transformation Details**: What happens at each step (joins, aggregations, calculations, filtering)
3. **Variable Lineage**: How variables are created, modified, and flow between datasets
4. **Business Logic**: Natural language description of what each transformation accomplishes

## Required Output Format:

```json
{{
  "lineage_nodes": [
    {{
      "dataset_name": "dataset_name",
      "dataset_type": "source|intermediate|final",
      "created_by": "operation_description",
      "inputs": ["input_dataset1", "input_dataset2"],
      "transformations": ["transformation_description1", "transformation_description2"],
      "variables_created": ["var1", "var2"],
      "business_logic": "Natural language description of what this dataset represents"
    }}
  ],
  "lineage_relationships": [
    {{
      "source_dataset": "source_dataset",
      "target_dataset": "target_dataset", 
      "transformation_type": "data_step|sql_join|aggregation|filtering",
      "transformation_description": "Detailed description of how source transforms to target",
      "variables_passed": ["variable1", "variable2"]
    }}
  ],
  "flow_description": "Overall narrative describing the complete data flow from sources to final outputs",
  "variable_lineage": {{
    "variable_name": ["source_variable1", "source_variable2"]
  }},
  "insights": {{
    "data_quality_steps": ["List of data quality/cleaning steps"],
    "business_transformations": ["List of business logic transformations"],
    "key_joins": ["Description of important joins"],
    "aggregation_levels": ["Description of aggregation levels"]
  }}
}}
```

## Analysis Guidelines:

1. **Identify Data Flow Patterns**:
   - Raw data sources (raw.*)
   - Cleaning/preparation steps (customers_clean, transactions_clean)  
   - Integration steps (joins, merges)
   - Business logic application (segmentation, calculations)
   - Final outputs and aggregations

2. **Understand Transformations**:
   - Data cleaning (handling nulls, data type conversions)
   - Variable derivations (calculated fields, concatenations)
   - Filtering and subsetting
   - Joins and their business meaning
   - Aggregations and their business purpose

3. **Trace Variable Lineage**:
   - Which source variables contribute to derived variables
   - How variables are transformed through the pipeline
   - New variables created at each step

4. **Provide Business Context**:
   - What business purpose does each transformation serve
   - What does each intermediate dataset represent
   - What insights can be derived from the final outputs

Focus on creating clear, comprehensive lineage that would help data analysts understand:
- Where their data comes from
- How it has been transformed
- What business rules have been applied
- How to trace data quality issues back to source

Generate the complete lineage analysis now:"""

        return prompt
    
    def _parse_llm_response(self, context: Dict[str, Any], llm_response: str) -> DatasetLineage:
        """Parse LLM response into DatasetLineage object"""
        
        # Try to parse actual LLM JSON response
        if llm_response.strip():
            try:
                # Extract JSON from LLM response (handle markdown code blocks)
                json_start = llm_response.find('{')
                json_end = llm_response.rfind('}') + 1
                
                if json_start != -1 and json_end > json_start:
                    json_str = llm_response[json_start:json_end]
                    llm_data = json.loads(json_str)
                    
                    # Parse nodes from LLM response
                    nodes = []
                    for node_data in llm_data.get('lineage_nodes', []):
                        nodes.append(LineageNode(
                            dataset_name=node_data.get('dataset_name', ''),
                            dataset_type=node_data.get('dataset_type', 'unknown'),
                            created_by=node_data.get('created_by', ''),
                            inputs=node_data.get('inputs', []),
                            transformations=node_data.get('transformations', []),
                            variables_created=node_data.get('variables_created', []),
                            business_logic=node_data.get('business_logic', '')
                        ))
                    
                    # Parse relationships from LLM response
                    relationships = []
                    for rel_data in llm_data.get('lineage_relationships', []):
                        relationships.append(LineageRelationship(
                            source_dataset=rel_data.get('source_dataset', ''),
                            target_dataset=rel_data.get('target_dataset', ''),
                            transformation_type=rel_data.get('transformation_type', ''),
                            transformation_description=rel_data.get('transformation_description', ''),
                            variables_passed=rel_data.get('variables_passed', [])
                        ))
                    
                    return DatasetLineage(
                        nodes=nodes,
                        relationships=relationships,
                        flow_description=llm_data.get('flow_description', ''),
                        variable_lineage=llm_data.get('variable_lineage', {})
                    )
                    
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                print(f"Warning: Could not parse LLM response as JSON: {e}")
                print("Falling back to sample response based on context...")
        
        # Fallback: create sample response based on context
        
        nodes = []
        relationships = []
        
        # Create nodes based on context
        for dataset in context['datasets']['source_datasets']:
            nodes.append(LineageNode(
                dataset_name=dataset,
                dataset_type='source',
                created_by='Raw data source',
                inputs=[],
                transformations=[],
                variables_created=[],
                business_logic=f'Source dataset {dataset} containing raw business data'
            ))
        
        for transformation in context['data_transformations']:
            for output in transformation['outputs']:
                transformations_desc = [t['description'] for t in transformation['transformations']]
                variables_created = list(transformation['variable_derivations'].keys())
                
                nodes.append(LineageNode(
                    dataset_name=output,
                    dataset_type='intermediate' if not output.endswith('_final') else 'final',
                    created_by=f"DATA step: {transformation['step_name']}",
                    inputs=transformation['inputs'],
                    transformations=transformations_desc,
                    variables_created=variables_created,
                    business_logic=f"Cleaned and processed version of {', '.join(transformation['inputs'])}"
                ))
                
                # Create relationships
                for input_ds in transformation['inputs']:
                    relationships.append(LineageRelationship(
                        source_dataset=input_ds,
                        target_dataset=output,
                        transformation_type='data_step',
                        transformation_description=f"Data cleaning and preparation: {', '.join(transformations_desc[:2])}...",
                        variables_passed=list(transformation['variable_derivations'].values())[:3]
                    ))
        
        for sql_op in context['sql_transformations']:
            output = sql_op['output_table']
            
            nodes.append(LineageNode(
                dataset_name=output,
                dataset_type='intermediate',
                created_by=f"PROC SQL: CREATE TABLE {output}",
                inputs=sql_op['inputs'],
                transformations=[f"SQL transformation with {len(sql_op['join_logic'])} joins"],
                variables_created=[col['alias'] for col in sql_op['select_columns'] if col['alias']],
                business_logic=f"Integrated dataset combining {', '.join(sql_op['inputs'])} with business calculations"
            ))
            
            # Create relationships for SQL operations
            for input_ds in sql_op['inputs']:
                relationships.append(LineageRelationship(
                    source_dataset=input_ds,
                    target_dataset=output,
                    transformation_type='sql_join',
                    transformation_description=f"SQL join and aggregation creating integrated dataset",
                    variables_passed=[col['expression'] for col in sql_op['select_columns'][:3]]
                ))
        
        # Create sample variable lineage
        variable_lineage = {}
        for transform in context['data_transformations']:
            for var, expr in transform['variable_derivations'].items():
                # Simple extraction of source variables from expression
                import re
                source_vars = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', expr)
                variable_lineage[var] = [v for v in source_vars if v not in ['sum', 'count', 'avg']][:3]
        
        return DatasetLineage(
            nodes=nodes,
            relationships=relationships,
            flow_description="Data flows from raw sources through cleaning and integration steps to create business-ready analytics datasets",
            variable_lineage=variable_lineage
        )


def generate_lineage_with_llm(context: LineageExtractionContext, 
                             llm_client=None) -> DatasetLineage:
    """
    Main function to generate dataset lineage using LLM
    
    Args:
        context: Extracted transformation context from AST
        llm_client: Optional LLM client (Claude, OpenAI, etc.)
        
    Returns:
        Complete dataset lineage with nodes and relationships
    """
    generator = LLMLineageGenerator()
    return generator.generate_lineage(context)


def print_lineage_summary(lineage: DatasetLineage):
    """Print formatted summary of dataset lineage"""
    
    print("=" * 80)
    print("📊 DATASET LINEAGE ANALYSIS")
    print("=" * 80)
    
    print(f"\n🔄 FLOW DESCRIPTION:")
    print(f"{lineage.flow_description}")
    
    print(f"\n📁 DATASET NODES ({len(lineage.nodes)}):")
    print("-" * 60)
    
    # Group nodes by type
    source_nodes = [n for n in lineage.nodes if n.dataset_type == 'source']
    intermediate_nodes = [n for n in lineage.nodes if n.dataset_type == 'intermediate']
    final_nodes = [n for n in lineage.nodes if n.dataset_type == 'final']
    
    print(f"\n  🗂️  SOURCE DATASETS ({len(source_nodes)}):")
    for node in source_nodes:
        print(f"    📥 {node.dataset_name}")
        print(f"       {node.business_logic}")
    
    print(f"\n  ⚙️  INTERMEDIATE DATASETS ({len(intermediate_nodes)}):")
    for node in intermediate_nodes:
        print(f"    🔄 {node.dataset_name}")
        print(f"       Created by: {node.created_by}")
        print(f"       From: {', '.join(node.inputs)}")
        print(f"       Transformations: {len(node.transformations)}")
        if node.variables_created:
            print(f"       New variables: {', '.join(node.variables_created[:3])}...")
        print(f"       Business logic: {node.business_logic}")
        print()
    
    print(f"\n  🎯 FINAL DATASETS ({len(final_nodes)}):")
    for node in final_nodes:
        print(f"    📤 {node.dataset_name}")
        print(f"       {node.business_logic}")
    
    print(f"\n🔗 LINEAGE RELATIONSHIPS ({len(lineage.relationships)}):")
    print("-" * 60)
    for rel in lineage.relationships:
        print(f"  {rel.source_dataset} ➜ {rel.target_dataset}")
        print(f"    Type: {rel.transformation_type}")
        print(f"    Description: {rel.transformation_description}")
        if rel.variables_passed:
            print(f"    Variables: {', '.join(rel.variables_passed[:3])}...")
        print()
    
    if lineage.variable_lineage:
        print(f"🧬 VARIABLE LINEAGE ({len(lineage.variable_lineage)} variables):")
        print("-" * 60)
        for var, sources in list(lineage.variable_lineage.items())[:5]:  # Show first 5
            print(f"  {var} ← {', '.join(sources)}")
        
        if len(lineage.variable_lineage) > 5:
            print(f"  ... and {len(lineage.variable_lineage) - 5} more variables")


# Example usage function for notebook
def create_lineage_example():
    """Example function showing how to use the lineage generator"""
    
    # This would typically come from your enhanced AST walker
    from enhanced_ast_walker import extract_comprehensive_context
    
    # Sample context (you would get this from extract_comprehensive_context)
    sample_context = LineageExtractionContext()
    
    # Generate lineage
    lineage = generate_lineage_with_llm(sample_context)
    
    # Display results
    print_lineage_summary(lineage)
    
    return lineage