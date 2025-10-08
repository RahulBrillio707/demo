"""
SAS AST Walker for Dataset Extraction

This module provides functions to walk through a SAS AST and extract:
- Source datasets (input datasets being read)
- Reference tables (tables referenced in SQL queries)
- Derived datasets (output datasets being created)
"""

from typing import Dict, List, Set, Any, Optional
from dataclasses import dataclass


@dataclass
class DatasetInfo:
    """Information about a dataset found in the AST"""
    name: str
    type: str  # 'source', 'derived', 'reference'
    operation: str  # 'data_step', 'proc_sql', 'set_statement', 'from_clause', etc.
    location: Dict[str, Any]  # Location information from AST
    node_id: str


class SASASTWalker:
    """Walker class to extract dataset information from SAS AST"""
    
    def __init__(self):
        self.source_datasets: Set[str] = set()
        self.derived_datasets: Set[str] = set() 
        self.reference_datasets: Set[str] = set()
        self.dataset_info: List[DatasetInfo] = []
    
    def walk_ast(self, ast_node: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Walk through the AST and extract all dataset information
        
        Args:
            ast_node: The root AST node (SasModule)
            
        Returns:
            Dictionary with 'source', 'derived', and 'reference' dataset lists
        """
        self._reset()
        self._walk_node(ast_node)
        
        return {
            'source_datasets': sorted(list(self.source_datasets)),
            'derived_datasets': sorted(list(self.derived_datasets)),
            'reference_datasets': sorted(list(self.reference_datasets)),
            'dataset_details': self.dataset_info
        }
    
    def _reset(self):
        """Reset all internal collections"""
        self.source_datasets.clear()
        self.derived_datasets.clear()
        self.reference_datasets.clear()
        self.dataset_info.clear()
    
    def _walk_node(self, node: Dict[str, Any]):
        """Recursively walk through AST nodes"""
        if not isinstance(node, dict):
            return
        
        node_type = node.get('type', '')
        
        # Process different node types
        if node_type == 'SasDataStep':
            self._process_data_step(node)
        elif node_type == 'SasSetStatement':
            self._process_set_statement(node)
        elif node_type == 'SasProcSqlCreateTableStatement':
            self._process_proc_sql_create_table(node)
        elif node_type == 'SasProcSqlFromClause':
            self._process_from_clause(node)
        elif node_type == 'SasProcStep':
            self._process_proc_step(node)
        elif node_type == 'SasMacroCall':
            self._process_macro_call(node)
        
        # Recursively process child nodes
        self._walk_children(node)
    
    def _walk_children(self, node: Dict[str, Any]):
        """Walk through all child nodes"""
        for key, value in node.items():
            if key in ['type', 'id', 'level', 'location', 'comments']:
                continue
            
            if isinstance(value, dict):
                self._walk_node(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self._walk_node(item)
    
    def _process_data_step(self, node: Dict[str, Any]):
        """Process SasDataStep nodes to extract derived datasets"""
        datasets_out = node.get('datasets_out', [])
        name = node.get('name', '')
        
        # Add derived datasets
        for dataset in datasets_out:
            if dataset:
                self.derived_datasets.add(dataset)
                self.dataset_info.append(DatasetInfo(
                    name=dataset,
                    type='derived',
                    operation='data_step',
                    location=node.get('location', {}),
                    node_id=node.get('id', '')
                ))
        
        # Also check if name is different from datasets_out
        if name and name not in datasets_out:
            self.derived_datasets.add(name)
            self.dataset_info.append(DatasetInfo(
                name=name,
                type='derived',
                operation='data_step',
                location=node.get('location', {}),
                node_id=node.get('id', '')
            ))
    
    def _process_set_statement(self, node: Dict[str, Any]):
        """Process SasSetStatement nodes to extract source datasets"""
        datasets = node.get('datasets', [])
        
        for dataset_ref in datasets:
            if isinstance(dataset_ref, dict) and dataset_ref.get('type') == 'SasDatasetRef':
                dataset_name = dataset_ref.get('name', '')
                if dataset_name:
                    self.source_datasets.add(dataset_name)
                    self.dataset_info.append(DatasetInfo(
                        name=dataset_name,
                        type='source',
                        operation='set_statement',
                        location=node.get('location', {}),
                        node_id=node.get('id', '')
                    ))
    
    def _process_proc_sql_create_table(self, node: Dict[str, Any]):
        """Process PROC SQL CREATE TABLE statements"""
        table_name = node.get('table_name', '')
        
        if table_name:
            self.derived_datasets.add(table_name)
            self.dataset_info.append(DatasetInfo(
                name=table_name,
                type='derived',
                operation='proc_sql_create_table',
                location=node.get('location', {}),
                node_id=node.get('id', '')
            ))
    
    def _process_from_clause(self, node: Dict[str, Any]):
        """Process FROM clauses in SQL to extract referenced tables"""
        # Main table
        main_table = node.get('main_table', {})
        if isinstance(main_table, dict):
            table_name = main_table.get('table_name', '')
            if table_name:
                self.reference_datasets.add(table_name)
                self.dataset_info.append(DatasetInfo(
                    name=table_name,
                    type='reference',
                    operation='from_clause_main',
                    location=node.get('location', {}),
                    node_id=node.get('id', '')
                ))
        
        # Joined tables
        joins = node.get('joins', [])
        for join in joins:
            if isinstance(join, dict):
                table = join.get('table', {})
                if isinstance(table, dict):
                    table_name = table.get('table_name', '')
                    if table_name:
                        self.reference_datasets.add(table_name)
                        self.dataset_info.append(DatasetInfo(
                            name=table_name,
                            type='reference',
                            operation='from_clause_join',
                            location=node.get('location', {}),
                            node_id=node.get('id', '')
                        ))
    
    def _process_proc_step(self, node: Dict[str, Any]):
        """Process PROC steps to identify implicit datasets"""
        procedure_name = node.get('procedure_name', '')
        
        if procedure_name == 'summary':
            # PROC SUMMARY typically works on the most recent dataset
            # Look for CLASS statement or implicit input
            # For now, we'll note this as a potential reference
            pass
        elif procedure_name == 'export':
            # PROC EXPORT reads from a dataset (usually specified in data= option)
            # The AST might not explicitly show this, but we can infer it
            pass
    
    def _process_macro_call(self, node: Dict[str, Any]):
        """Process macro calls that might involve datasets"""
        macro_name = node.get('macro_name', '')
        arguments = node.get('arguments', {})
        
        if macro_name and arguments:
            # Extract positional arguments that might be dataset names
            positional = arguments.get('positional', [])
            for arg in positional:
                if isinstance(arg, str) and arg != ',' and not arg.startswith('"'):
                    # This might be a dataset name
                    self.reference_datasets.add(arg)
                    self.dataset_info.append(DatasetInfo(
                        name=arg,
                        type='reference',
                        operation='macro_call_argument',
                        location=node.get('location', {}),
                        node_id=node.get('id', '')
                    ))


def extract_datasets_from_ast(ast_data: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Convenience function to extract datasets from AST
    
    Args:
        ast_data: The complete AST data structure
        
    Returns:
        Dictionary with source, derived, and reference dataset information
    """
    walker = SASASTWalker()
    return walker.walk_ast(ast_data)


def print_dataset_summary(results: Dict[str, List[str]]):
    """Print a formatted summary of the dataset analysis"""
    print("=" * 60)
    print("SAS DATASET ANALYSIS SUMMARY")
    print("=" * 60)
    
    print(f"\nSOURCE DATASETS ({len(results['source_datasets'])}):")
    print("-" * 40)
    for dataset in results['source_datasets']:
        print(f"  📥 {dataset}")
    
    print(f"\nDERIVED DATASETS ({len(results['derived_datasets'])}):")
    print("-" * 40)
    for dataset in results['derived_datasets']:
        print(f"  📤 {dataset}")
    
    print(f"\nREFERENCE DATASETS ({len(results['reference_datasets'])}):")
    print("-" * 40)
    for dataset in results['reference_datasets']:
        print(f"  🔗 {dataset}")
    
    if 'dataset_details' in results:
        print(f"\nDETAILED INFORMATION ({len(results['dataset_details'])}):")
        print("-" * 40)
        for info in results['dataset_details']:
            location = info.location
            line_info = f"Line {location.get('line_start', 'N/A')}" if location else "N/A"
            print(f"  {info.name} | {info.type} | {info.operation} | {line_info}")


# Example usage
if __name__ == "__main__":
    # Example with the provided AST data
    sample_ast = {
        # ... your AST data would go here
    }
    
    results = extract_datasets_from_ast(sample_ast)
    print_dataset_summary(results)