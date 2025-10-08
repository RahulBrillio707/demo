"""
Enhanced SAS AST Walker for Comprehensive Transformation Details Extraction

This module provides detailed extraction of transformation logic, dependencies,
and context information from SAS AST for LLM-based lineage generation.
"""

from typing import Dict, List, Set, Any, Optional, Tuple
from dataclasses import dataclass, field
import json


@dataclass
class TransformationDetail:
    """Detailed transformation information"""
    transformation_type: str  # 'assignment', 'conditional', 'filter', 'calculation'
    description: str
    variables_involved: List[str]
    logic: str
    location: Dict[str, Any]


@dataclass
class DataStepContext:
    """Complete context for a DATA step"""
    step_id: str
    step_name: str
    input_datasets: List[str]
    output_datasets: List[str]
    transformations: List[TransformationDetail]
    filtering_logic: List[str]
    variable_derivations: Dict[str, str]  # variable_name -> expression
    conditional_logic: List[str]
    location: Dict[str, Any]
    sequence_order: int


@dataclass
class SqlTransformationContext:
    """Complete context for PROC SQL operations"""
    step_id: str
    output_table: str
    input_tables: List[str]
    select_columns: List[Dict[str, str]]  # expression, alias, type
    join_logic: List[Dict[str, str]]  # join_type, table, condition
    where_conditions: List[str]
    group_by_columns: List[str]
    calculated_fields: List[Dict[str, Any]]  # CASE statements, functions
    aggregations: List[Dict[str, str]]  # function, column, alias
    location: Dict[str, Any]
    sequence_order: int


@dataclass
class MacroContext:
    """Macro definition and usage context"""
    macro_name: str
    parameters: List[str]
    macro_operations: List[Any]  # Operations within macro body
    call_instances: List[Dict[str, Any]]  # Where macro is called with parameters
    location: Dict[str, Any]


@dataclass
class ProcedureContext:
    """Context for other PROC steps"""
    step_id: str
    procedure_name: str
    input_datasets: List[str]  # Inferred or explicit
    output_datasets: List[str]  # Inferred or explicit
    operation_details: Dict[str, Any]
    implicit_behavior: str  # Description of what this proc does
    location: Dict[str, Any]
    sequence_order: int


@dataclass
class LineageExtractionContext:
    """Complete context for LLM lineage generation"""
    data_steps: List[DataStepContext] = field(default_factory=list)
    sql_operations: List[SqlTransformationContext] = field(default_factory=list)
    macro_definitions: List[MacroContext] = field(default_factory=list)
    procedure_steps: List[ProcedureContext] = field(default_factory=list)
    operation_sequence: List[str] = field(default_factory=list)
    dataset_summary: Dict[str, List[str]] = field(default_factory=dict)
    variable_lineage: Dict[str, List[str]] = field(default_factory=dict)  # variable -> source_variables
    implicit_relationships: List[str] = field(default_factory=list)


class EnhancedSASASTWalker:
    """Enhanced walker to extract comprehensive transformation details"""
    
    def __init__(self):
        self.context = LineageExtractionContext()
        self.sequence_counter = 0
        self.current_variables = set()  # Track variables in scope
        
    def extract_comprehensive_context(self, ast_node: Dict[str, Any]) -> LineageExtractionContext:
        """Extract complete transformation context from AST"""
        self._reset()
        self._walk_node(ast_node)
        self._build_dataset_summary()
        self._infer_implicit_relationships()
        return self.context
    
    def _reset(self):
        """Reset internal state"""
        self.context = LineageExtractionContext()
        self.sequence_counter = 0
        self.current_variables.clear()
    
    def _walk_node(self, node: Dict[str, Any]):
        """Walk through AST nodes and extract details"""
        if not isinstance(node, dict):
            return
        
        node_type = node.get('type', '')
        
        # Process different node types with detailed extraction
        if node_type == 'SasDataStep':
            self._extract_data_step_details(node)
        elif node_type == 'SasProcSqlCreateTableStatement':
            self._extract_sql_details(node)
        elif node_type == 'SasMacroDef':
            self._extract_macro_definition(node)
        elif node_type == 'SasMacroCall':
            self._extract_macro_call(node)
        elif node_type == 'SasProcStep':
            self._extract_procedure_details(node)
        
        # Continue walking child nodes
        self._walk_children(node)
    
    def _walk_children(self, node: Dict[str, Any]):
        """Recursively walk child nodes"""
        for key, value in node.items():
            if key in ['type', 'id', 'level', 'location', 'comments']:
                continue
            
            if isinstance(value, dict):
                self._walk_node(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self._walk_node(item)
    
    def _extract_data_step_details(self, node: Dict[str, Any]):
        """Extract comprehensive DATA step transformation details"""
        self.sequence_counter += 1
        
        # Basic info
        step_id = node.get('id', f'datastep_{self.sequence_counter}')
        step_name = node.get('name', 'unnamed')
        output_datasets = node.get('datasets_out', [step_name] if step_name else [])
        location = node.get('location', {})
        
        # Extract input datasets from SET statements
        input_datasets = []
        transformations = []
        filtering_logic = []
        variable_derivations = {}
        conditional_logic = []
        
        statements = node.get('statements', [])
        
        for stmt in statements:
            stmt_type = stmt.get('type', '')
            
            if stmt_type == 'SasSetStatement':
                # Input datasets
                datasets = stmt.get('datasets', [])
                for ds in datasets:
                    if isinstance(ds, dict) and ds.get('type') == 'SasDatasetRef':
                        input_datasets.append(ds.get('name', ''))
            
            elif stmt_type == 'SasAssignmentStatement':
                # Variable derivations
                variable = stmt.get('variable', '')
                expression = stmt.get('expression', '')
                variable_derivations[variable] = expression
                
                # Extract variables involved in expression
                involved_vars = self._extract_variables_from_expression(expression)
                
                transformations.append(TransformationDetail(
                    transformation_type='assignment',
                    description=f'{variable} = {expression}',
                    variables_involved=[variable] + involved_vars,
                    logic=f'{variable} = {expression}',
                    location=stmt.get('location', {})
                ))
                
                self.current_variables.add(variable)
            
            elif stmt_type == 'SasIfStatement':
                # Conditional logic
                condition = stmt.get('condition', '')
                then_block = stmt.get('then_block', {})
                else_block = stmt.get('else_block', {})
                
                conditional_desc = f"if {condition}"
                if then_block:
                    then_logic = self._extract_statement_logic(then_block)
                    conditional_desc += f" then {then_logic}"
                if else_block:
                    else_logic = self._extract_statement_logic(else_block)
                    conditional_desc += f" else {else_logic}"
                
                conditional_logic.append(conditional_desc)
                
                transformations.append(TransformationDetail(
                    transformation_type='conditional',
                    description=conditional_desc,
                    variables_involved=self._extract_variables_from_expression(condition),
                    logic=conditional_desc,
                    location=stmt.get('location', {})
                ))
            
            elif stmt_type == 'SasUnknownStatement':
                # Handle WHERE clauses, FORMAT statements, etc.
                text = stmt.get('text', '').strip()
                if text.lower().startswith('where'):
                    filtering_logic.append(text)
                    transformations.append(TransformationDetail(
                        transformation_type='filter',
                        description=text,
                        variables_involved=self._extract_variables_from_expression(text),
                        logic=text,
                        location=stmt.get('location', {})
                    ))
        
        # Create DATA step context
        data_step_context = DataStepContext(
            step_id=step_id,
            step_name=step_name,
            input_datasets=input_datasets,
            output_datasets=output_datasets,
            transformations=transformations,
            filtering_logic=filtering_logic,
            variable_derivations=variable_derivations,
            conditional_logic=conditional_logic,
            location=location,
            sequence_order=self.sequence_counter
        )
        
        self.context.data_steps.append(data_step_context)
        self.context.operation_sequence.append(f"DATA_{step_name}")
    
    def _extract_sql_details(self, node: Dict[str, Any]):
        """Extract comprehensive PROC SQL transformation details"""
        self.sequence_counter += 1
        
        step_id = node.get('id', f'sql_{self.sequence_counter}')
        output_table = node.get('table_name', '')
        location = node.get('location', {})
        
        select_statement = node.get('select_statement', {})
        
        # Extract SELECT columns
        select_columns = []
        calculated_fields = []
        aggregations = []
        
        columns = select_statement.get('columns', [])
        for col in columns:
            col_type = col.get('type', '')
            expression = col.get('expression', '')
            alias = col.get('alias', '')
            
            col_info = {
                'expression': expression,
                'alias': alias,
                'type': col_type
            }
            select_columns.append(col_info)
            
            # Check for calculated fields (CASE statements)
            if col_type == 'SasCaseColumn':
                case_details = {
                    'type': 'case_statement',
                    'when_clauses': col.get('when_clauses', []),
                    'else_value': col.get('else_value', ''),
                    'alias': alias
                }
                calculated_fields.append(case_details)
            
            # Check for aggregations
            elif any(func in expression.lower() for func in ['sum', 'count', 'avg', 'max', 'min']):
                agg_info = {
                    'function': expression,
                    'column': expression,
                    'alias': alias
                }
                aggregations.append(agg_info)
        
        # Extract FROM clause and JOINs
        from_clause = select_statement.get('from', {})
        input_tables = []
        join_logic = []
        
        if from_clause:
            main_table = from_clause.get('main_table', {})
            if main_table:
                main_table_name = main_table.get('table_name', '')
                if main_table_name:
                    input_tables.append(main_table_name)
            
            joins = from_clause.get('joins', [])
            for join in joins:
                join_type = join.get('join_type', 'INNER')
                table = join.get('table', {})
                table_name = table.get('table_name', '') if table else ''
                condition = join.get('on_condition', {})
                condition_text = condition.get('condition', '') if condition else ''
                
                if table_name:
                    input_tables.append(table_name)
                
                join_info = {
                    'join_type': join_type,
                    'table': table_name,
                    'condition': condition_text
                }
                join_logic.append(join_info)
        
        # Extract GROUP BY
        group_by = select_statement.get('group_by', {})
        group_by_columns = []
        if group_by:
            columns = group_by.get('columns', [])
            for col in columns:
                if isinstance(col, dict):
                    group_by_columns.append(col.get('name', ''))
                else:
                    group_by_columns.append(str(col))
        
        # Extract WHERE conditions
        where_conditions = []
        where_clause = select_statement.get('where')
        if where_clause:
            # WHERE clause extraction would need more detailed parsing
            where_conditions.append("WHERE clause present")
        
        # Create SQL context
        sql_context = SqlTransformationContext(
            step_id=step_id,
            output_table=output_table,
            input_tables=input_tables,
            select_columns=select_columns,
            join_logic=join_logic,
            where_conditions=where_conditions,
            group_by_columns=group_by_columns,
            calculated_fields=calculated_fields,
            aggregations=aggregations,
            location=location,
            sequence_order=self.sequence_counter
        )
        
        self.context.sql_operations.append(sql_context)
        self.context.operation_sequence.append(f"SQL_CREATE_{output_table}")
    
    def _extract_macro_definition(self, node: Dict[str, Any]):
        """Extract macro definition details"""
        macro_name = node.get('macro_name', '')
        parameters = [p.get('name', '') for p in node.get('parameters', [])]
        macro_body = node.get('macro_body', [])
        location = node.get('location', {})
        
        # Extract operations within macro body
        macro_operations = []
        for operation in macro_body:
            op_type = operation.get('type', '')
            if op_type in ['SasDataStep', 'SasProcStep']:
                macro_operations.append({
                    'type': op_type,
                    'details': self._extract_operation_summary(operation)
                })
        
        macro_context = MacroContext(
            macro_name=macro_name,
            parameters=parameters,
            macro_operations=macro_operations,
            call_instances=[],  # Will be populated when calls are found
            location=location
        )
        
        self.context.macro_definitions.append(macro_context)
    
    def _extract_macro_call(self, node: Dict[str, Any]):
        """Extract macro call details"""
        macro_name = node.get('macro_name', '')
        arguments = node.get('arguments', {})
        positional = arguments.get('positional', [])
        
        # Find corresponding macro definition
        for macro_def in self.context.macro_definitions:
            if macro_def.macro_name == macro_name:
                call_info = {
                    'arguments': positional,
                    'location': node.get('location', {})
                }
                macro_def.call_instances.append(call_info)
                break
        
        self.context.operation_sequence.append(f"MACRO_CALL_{macro_name}")
    
    def _extract_procedure_details(self, node: Dict[str, Any]):
        """Extract details for other PROC steps"""
        self.sequence_counter += 1
        
        step_id = node.get('id', f'proc_{self.sequence_counter}')
        procedure_name = node.get('procedure_name', '')
        location = node.get('location', {})
        
        # Infer input/output based on procedure type
        input_datasets = []
        output_datasets = []
        implicit_behavior = ""
        operation_details = {}
        
        if procedure_name == 'summary':
            implicit_behavior = "Calculates summary statistics and creates output dataset"
            # PROC SUMMARY typically uses the most recently created dataset
            if self.context.data_steps or self.context.sql_operations:
                # Find most recent dataset
                recent_outputs = []
                if self.context.sql_operations:
                    recent_outputs.extend([op.output_table for op in self.context.sql_operations])
                if self.context.data_steps:
                    for ds in self.context.data_steps:
                        recent_outputs.extend(ds.output_datasets)
                if recent_outputs:
                    input_datasets = [recent_outputs[-1]]  # Most recent
            
            # Extract variables being summarized
            statements = node.get('statements', [])
            for stmt in statements:
                if stmt.get('type') == 'SasProcVarStatement':
                    variables = stmt.get('variables', [])
                    operation_details['summary_variables'] = variables
        
        elif procedure_name == 'export':
            implicit_behavior = "Exports dataset to external file"
            options = node.get('options', {})
            if 'dbms' in options:
                operation_details['export_format'] = options['dbms']
        
        proc_context = ProcedureContext(
            step_id=step_id,
            procedure_name=procedure_name,
            input_datasets=input_datasets,
            output_datasets=output_datasets,
            operation_details=operation_details,
            implicit_behavior=implicit_behavior,
            location=location,
            sequence_order=self.sequence_counter
        )
        
        self.context.procedure_steps.append(proc_context)
        self.context.operation_sequence.append(f"PROC_{procedure_name.upper()}")
    
    def _extract_variables_from_expression(self, expression: str) -> List[str]:
        """Extract variable names from an expression"""
        # Simple extraction - could be enhanced with proper parsing
        variables = []
        if not expression:
            return variables
        
        # Common patterns to extract variables
        import re
        # Remove function calls and operators to find variables
        cleaned = re.sub(r'\b(sum|count|avg|max|min|missing|today|month|year)\s*\([^)]*\)', '', expression.lower())
        cleaned = re.sub(r'[+\-*/=<>(),\'"]', ' ', cleaned)
        
        tokens = cleaned.split()
        for token in tokens:
            token = token.strip()
            # Filter out numbers, keywords, and operators
            if (token and not token.isdigit() and 
                token not in ['and', 'or', 'then', 'else', 'if', 'where'] and
                not token.startswith('&')):  # Exclude macro variables
                variables.append(token)
        
        return list(set(variables))  # Remove duplicates
    
    def _extract_statement_logic(self, stmt: Dict[str, Any]) -> str:
        """Extract logic from a statement block"""
        if not stmt:
            return ""
        
        stmt_type = stmt.get('type', '')
        if stmt_type == 'SasAssignmentStatement':
            variable = stmt.get('variable', '')
            expression = stmt.get('expression', '')
            return f"{variable} = {expression}"
        
        return str(stmt)
    
    def _extract_operation_summary(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """Extract summary of an operation for macro context"""
        op_type = operation.get('type', '')
        
        if op_type == 'SasDataStep':
            return {
                'name': operation.get('name', ''),
                'outputs': operation.get('datasets_out', [])
            }
        elif op_type == 'SasProcStep':
            return {
                'procedure': operation.get('procedure_name', ''),
                'location': operation.get('location', {})
            }
        
        return {}
    
    def _build_dataset_summary(self):
        """Build summary of all datasets by type"""
        source_datasets = set()
        derived_datasets = set()
        reference_datasets = set()
        
        # From DATA steps
        for ds in self.context.data_steps:
            source_datasets.update(ds.input_datasets)
            derived_datasets.update(ds.output_datasets)
        
        # From SQL operations
        for sql in self.context.sql_operations:
            reference_datasets.update(sql.input_tables)
            derived_datasets.add(sql.output_table)
        
        # From procedures
        for proc in self.context.procedure_steps:
            reference_datasets.update(proc.input_datasets)
            derived_datasets.update(proc.output_datasets)
        
        # Source datasets are those that are referenced but never created
        true_sources = source_datasets - derived_datasets
        
        self.context.dataset_summary = {
            'source_datasets': sorted(list(true_sources)),
            'derived_datasets': sorted(list(derived_datasets)),
            'reference_datasets': sorted(list(reference_datasets))
        }
    
    def _infer_implicit_relationships(self):
        """Infer implicit relationships between operations"""
        relationships = []
        
        # PROC SUMMARY typically works on the most recent dataset
        for proc in self.context.procedure_steps:
            if proc.procedure_name == 'summary' and not proc.input_datasets:
                relationships.append(f"PROC SUMMARY implicitly uses most recent dataset for input")
        
        # Sequential dependencies
        prev_outputs = []
        for i, op in enumerate(self.context.operation_sequence):
            if i > 0:
                relationships.append(f"{op} follows {self.context.operation_sequence[i-1]} in sequence")
        
        self.context.implicit_relationships = relationships


def extract_comprehensive_context(ast_data: Dict[str, Any]) -> LineageExtractionContext:
    """
    Extract comprehensive transformation context from SAS AST
    
    Args:
        ast_data: Complete AST data structure
        
    Returns:
        LineageExtractionContext with all transformation details
    """
    walker = EnhancedSASASTWalker()
    return walker.extract_comprehensive_context(ast_data)


def print_comprehensive_summary(context: LineageExtractionContext):
    """Print detailed summary of extracted context"""
    print("=" * 80)
    print("COMPREHENSIVE SAS TRANSFORMATION CONTEXT")
    print("=" * 80)
    
    print(f"\nDATA STEPS ({len(context.data_steps)}):")
    print("-" * 50)
    for ds in context.data_steps:
        print(f"  📝 {ds.step_name}")
        print(f"     Inputs: {ds.input_datasets}")
        print(f"     Outputs: {ds.output_datasets}")
        print(f"     Transformations: {len(ds.transformations)}")
        print(f"     Variables derived: {len(ds.variable_derivations)}")
        print()
    
    print(f"SQL OPERATIONS ({len(context.sql_operations)}):")
    print("-" * 50)
    for sql in context.sql_operations:
        print(f"  🔍 CREATE TABLE {sql.output_table}")
        print(f"     From tables: {sql.input_tables}")
        print(f"     Select columns: {len(sql.select_columns)}")
        print(f"     Joins: {len(sql.join_logic)}")
        print(f"     Calculated fields: {len(sql.calculated_fields)}")
        print()
    
    print(f"MACRO DEFINITIONS ({len(context.macro_definitions)}):")
    print("-" * 50)
    for macro in context.macro_definitions:
        print(f"  🔧 %{macro.macro_name}")
        print(f"     Parameters: {macro.parameters}")
        print(f"     Operations in body: {len(macro.macro_operations)}")
        print(f"     Called: {len(macro.call_instances)} times")
        print()
    
    print(f"PROCEDURE STEPS ({len(context.procedure_steps)}):")
    print("-" * 50)
    for proc in context.procedure_steps:
        print(f"  ⚙️  PROC {proc.procedure_name.upper()}")
        print(f"     Implicit behavior: {proc.implicit_behavior}")
        print(f"     Input datasets: {proc.input_datasets}")
        print(f"     Output datasets: {proc.output_datasets}")
        print()
    
    print("OPERATION SEQUENCE:")
    print("-" * 50)
    for i, op in enumerate(context.operation_sequence, 1):
        print(f"  {i:2d}. {op}")
    
    print(f"\nDATASET SUMMARY:")
    print("-" * 50)
    for category, datasets in context.dataset_summary.items():
        print(f"  {category}: {datasets}")


# Example usage
if __name__ == "__main__":
    # Test with sample AST data
    pass