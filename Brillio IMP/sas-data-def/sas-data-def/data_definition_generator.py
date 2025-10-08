"""
Data Definition Generator for Source Tables

This module creates comprehensive data definitions for source datasets by:
1. Scanning source directory for CSV files
2. Matching CSV files with source datasets from lineage
3. Analyzing CSV structure and sample data
4. Using LLM to generate detailed data definitions with business context
"""

import os
import pandas as pd
import json
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from pathlib import Path
import numpy as np
from langchain_openai import AzureChatOpenAI

from llm_lineage_generator import DatasetLineage, LineageNode


@dataclass
class ColumnDefinition:
    """Definition for a single column"""
    column_name: str
    data_type: str
    max_length: Optional[int] = None
    is_nullable: bool = True
    constraints: List[str] = field(default_factory=list)
    description: str = ""
    business_rules: List[str] = field(default_factory=list)
    sample_values: List[str] = field(default_factory=list)


@dataclass
class TableDefinition:
    """Complete definition for a table"""
    table_name: str
    file_path: str
    business_purpose: str
    columns: List[ColumnDefinition]
    primary_keys: List[str] = field(default_factory=list)
    foreign_keys: List[Dict[str, str]] = field(default_factory=list)
    indexes: List[str] = field(default_factory=list)
    row_count: int = 0
    data_quality_notes: List[str] = field(default_factory=list)
    business_context: str = ""


@dataclass
class DataDefinitionContext:
    """Context for LLM data definition generation"""
    table_definitions: List[TableDefinition] = field(default_factory=list)
    generation_metadata: Dict[str, Any] = field(default_factory=dict)


class DataDefinitionGenerator:
    """Generate comprehensive data definitions using LLM analysis"""
    
    def __init__(self, llm_client: Optional[AzureChatOpenAI] = None):
        self.llm_client = llm_client or self._get_llm_client()
        self.supported_extensions = {'.csv', '.tsv', '.txt'}
    
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
    
    def generate_data_definitions(self, 
                                source_directory: str,
                                lineage: DatasetLineage,
                                sample_size: int = 100) -> DataDefinitionContext:
        """
        Generate data definitions for source datasets
        
        Args:
            source_directory: Directory containing CSV files
            lineage: Dataset lineage containing source dataset info
            sample_size: Number of rows to sample for analysis
            
        Returns:
            DataDefinitionContext with generated definitions
        """
        
        # Step 1: Discover CSV files in directory
        csv_files = self._discover_csv_files(source_directory)
        print(f"📁 Found {len(csv_files)} CSV files in {source_directory}")
        
        # Step 2: Match CSV files with source datasets from lineage
        source_datasets = self._get_source_datasets(lineage)
        matched_files = self._match_files_to_datasets(csv_files, source_datasets)
        
        print(f"🔗 Matched {len(matched_files)} CSV files to source datasets")
        
        # Step 3: Analyze each matched file and generate definitions
        table_definitions = []
        
        for file_path, dataset_info in matched_files.items():
            print(f"📊 Analyzing {os.path.basename(file_path)}...")
            
            try:
                # Load and analyze CSV
                df = pd.read_csv(file_path, nrows=sample_size * 2)  # Read more for better analysis
                
                # Generate definition using LLM
                table_def = self._generate_table_definition(
                    file_path=file_path,
                    dataframe=df,
                    dataset_info=dataset_info,
                    sample_size=sample_size
                )
                
                table_definitions.append(table_def)
                print(f"✅ Generated definition for {table_def.table_name}")
                
            except Exception as e:
                print(f"❌ Error analyzing {file_path}: {e}")
                continue
        
        # Step 4: Create comprehensive context
        context = DataDefinitionContext(
            table_definitions=table_definitions,
            generation_metadata={
                "source_directory": source_directory,
                "total_files_found": len(csv_files),
                "files_matched": len(matched_files),
                "definitions_generated": len(table_definitions),
                "sample_size_used": sample_size
            }
        )
        
        return context
    
    def _discover_csv_files(self, directory: str) -> List[str]:
        """Discover all CSV files in directory"""
        csv_files = []
        
        for root, dirs, files in os.walk(directory):
            for file in files:
                if Path(file).suffix.lower() in self.supported_extensions:
                    csv_files.append(os.path.join(root, file))
        
        return sorted(csv_files)
    
    def _get_source_datasets(self, lineage: DatasetLineage) -> List[LineageNode]:
        """Extract source datasets from lineage"""
        return [node for node in lineage.nodes if node.dataset_type == 'source']
    
    def _match_files_to_datasets(self, 
                                csv_files: List[str], 
                                source_datasets: List[LineageNode]) -> Dict[str, LineageNode]:
        """Match CSV files to source datasets based on naming patterns"""
        matched = {}
        
        for csv_file in csv_files:
            file_name = os.path.basename(csv_file).lower()
            file_stem = Path(csv_file).stem.lower()
            
            # Try to match with source dataset names
            for dataset in source_datasets:
                dataset_name = dataset.dataset_name.lower()
                
                # Remove common prefixes like 'raw.'
                clean_dataset_name = dataset_name.replace('raw.', '').replace('source.', '')
                
                # Check various matching patterns
                if (clean_dataset_name in file_stem or
                    file_stem in clean_dataset_name or
                    self._fuzzy_match(clean_dataset_name, file_stem)):
                    
                    matched[csv_file] = dataset
                    break
        
        return matched
    
    def _fuzzy_match(self, name1: str, name2: str, threshold: float = 0.7) -> bool:
        """Simple fuzzy matching for dataset names"""
        # Replace underscores/hyphens and check similarity
        name1_clean = name1.replace('_', '').replace('-', '')
        name2_clean = name2.replace('_', '').replace('-', '')
        
        # Check if one contains the other or if they share significant overlap
        if len(name1_clean) == 0 or len(name2_clean) == 0:
            return False
        
        # Simple containment check
        if name1_clean in name2_clean or name2_clean in name1_clean:
            return True
        
        # Check common words
        words1 = set(name1_clean.split())
        words2 = set(name2_clean.split())
        
        if words1 and words2:
            overlap = len(words1.intersection(words2))
            union = len(words1.union(words2))
            return (overlap / union) >= threshold
        
        return False
    
    def _generate_table_definition(self,
                                 file_path: str,
                                 dataframe: pd.DataFrame,
                                 dataset_info: LineageNode,
                                 sample_size: int) -> TableDefinition:
        """Generate comprehensive table definition using LLM"""
        
        # Analyze dataframe structure
        analysis = self._analyze_dataframe(dataframe, sample_size)
        
        # Prepare context for LLM
        llm_context = self._prepare_definition_context(file_path, analysis, dataset_info)
        
        # Generate definition using LLM
        try:
            prompt = self._build_definition_prompt(llm_context)
            response = self.llm_client.invoke(prompt)
            llm_response = response.content
            
            # Parse LLM response
            table_def = self._parse_definition_response(
                llm_response, file_path, analysis, dataset_info
            )
            
        except Exception as e:
            print(f"Warning: LLM call failed for {file_path}: {e}")
            # Fallback to basic analysis
            table_def = self._create_fallback_definition(
                file_path, analysis, dataset_info
            )
        
        return table_def
    
    def _analyze_dataframe(self, df: pd.DataFrame, sample_size: int) -> Dict[str, Any]:
        """Perform detailed analysis of dataframe structure and content"""
        
        # Basic info
        analysis = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "dtypes": df.dtypes.to_dict(),
            "sample_data": df.head(min(sample_size, len(df))).to_dict('records'),
            "column_analysis": {}
        }
        
        # Analyze each column
        for col in df.columns:
            col_analysis = {
                "dtype": str(df[col].dtype),
                "non_null_count": df[col].count(),
                "null_count": df[col].isnull().sum(),
                "null_percentage": (df[col].isnull().sum() / len(df)) * 100,
                "unique_count": df[col].nunique(),
                "sample_values": []
            }
            
            # Get sample values (non-null)
            sample_values = df[col].dropna().head(10).astype(str).tolist()
            col_analysis["sample_values"] = sample_values
            
            # Analyze data patterns
            if df[col].dtype in ['object', 'string']:
                # String analysis
                col_analysis["max_length"] = df[col].astype(str).str.len().max()
                col_analysis["min_length"] = df[col].astype(str).str.len().min()
                col_analysis["avg_length"] = df[col].astype(str).str.len().mean()
                
                # Check for common patterns
                col_analysis["patterns"] = self._detect_patterns(df[col])
                
            elif df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                # Numeric analysis
                col_analysis["min_value"] = df[col].min()
                col_analysis["max_value"] = df[col].max()
                col_analysis["mean_value"] = df[col].mean()
                
                # Check if it could be a foreign key (limited unique values)
                if df[col].nunique() < len(df) * 0.8:
                    col_analysis["potential_foreign_key"] = True
            
            elif df[col].dtype in ['datetime64[ns]', 'datetime']:
                # Date analysis
                col_analysis["min_date"] = df[col].min()
                col_analysis["max_date"] = df[col].max()
            
            analysis["column_analysis"][col] = col_analysis
        
        return analysis
    
    def _detect_patterns(self, series: pd.Series) -> List[str]:
        """Detect common patterns in string data"""
        patterns = []
        sample_values = series.dropna().head(20).astype(str)
        
        # Check for email pattern
        if sample_values.str.contains('@').any():
            patterns.append("email")
        
        # Check for phone pattern
        if sample_values.str.contains(r'^\d{3}-?\d{3}-?\d{4}$', regex=True).any():
            patterns.append("phone")
        
        # Check for ID pattern (starts with letters, followed by numbers)
        if sample_values.str.contains(r'^[A-Z]{2,3}\d+$', regex=True).any():
            patterns.append("id_code")
        
        # Check for currency
        if sample_values.str.contains(r'^\$?\d+\.?\d*$', regex=True).any():
            patterns.append("currency")
        
        # Check for date strings
        if sample_values.str.contains(r'\d{4}-\d{2}-\d{2}').any():
            patterns.append("date_string")
        
        return patterns
    
    def _prepare_definition_context(self,
                                  file_path: str,
                                  analysis: Dict[str, Any],
                                  dataset_info: LineageNode) -> Dict[str, Any]:
        """Prepare context for LLM definition generation"""
        
        return {
            "file_info": {
                "file_name": os.path.basename(file_path),
                "file_path": file_path,
                "row_count": analysis["row_count"],
                "column_count": analysis["column_count"]
            },
            "business_context": {
                "dataset_name": dataset_info.dataset_name,
                "business_purpose": dataset_info.business_logic,
                "created_by": dataset_info.created_by
            },
            "data_structure": {
                "columns": analysis["columns"],
                "column_analysis": analysis["column_analysis"],
                "sample_data": analysis["sample_data"][:5]  # First 5 rows only
            }
        }
    
    def _build_definition_prompt(self, context: Dict[str, Any]) -> str:
        """Build prompt for LLM data definition generation"""
        
        prompt = f"""# Data Definition Generation Task

You are a data architect creating comprehensive data definitions for a source dataset.

## Dataset Context:
{json.dumps(context, indent=2, default=str)}

## Your Task:
Create a comprehensive data definition that includes:

1. **Table-level Information**:
   - Business purpose and context
   - Data quality assessment
   - Primary key identification
   - Potential foreign keys

2. **Column-level Definitions**:
   - Appropriate data types for database implementation
   - Maximum lengths for string columns
   - Null constraints based on data analysis
   - Business descriptions for each column
   - Validation rules and constraints
   - Sample values for reference

## Required Output Format:

```json
{{
  "table_definition": {{
    "table_name": "clean_table_name",
    "business_purpose": "Clear business description of what this table represents",
    "data_quality_notes": ["List of data quality observations"],
    "primary_keys": ["column1", "column2"],
    "foreign_keys": [
      {{"column": "customer_id", "references": "customers.id", "description": "Reference to customer table"}}
    ],
    "indexes": ["recommended_index_columns"],
    "business_context": "Detailed business context and usage"
  }},
  "column_definitions": [
    {{
      "column_name": "column_name",
      "data_type": "VARCHAR(50) | INTEGER | DECIMAL(10,2) | DATE | BOOLEAN",
      "max_length": 50,
      "is_nullable": false,
      "constraints": ["UNIQUE", "CHECK (column > 0)"],
      "description": "Business description of what this column represents",
      "business_rules": ["List of business rules for this column"],
      "sample_values": ["sample1", "sample2", "sample3"]
    }}
  ],
  "recommendations": {{
    "data_quality_improvements": ["Suggestions for data quality"],
    "additional_constraints": ["Suggested constraints not captured above"],
    "indexing_strategy": ["Indexing recommendations"],
    "business_insights": ["Business insights from data analysis"]
  }}
}}
```

## Analysis Guidelines:

1. **Data Type Selection**:
   - Use appropriate SQL data types (VARCHAR, INTEGER, DECIMAL, DATE, BOOLEAN)
   - Consider storage efficiency and query performance
   - Account for data growth and future requirements

2. **Constraint Identification**:
   - NOT NULL for columns with no missing values and business criticality
   - UNIQUE for columns that should be unique
   - CHECK constraints for business rules (ranges, formats)
   - Foreign key relationships based on naming patterns and data

3. **Business Context**:
   - Provide clear, business-friendly descriptions
   - Explain the purpose and usage of each column
   - Identify key business rules and relationships

4. **Data Quality Assessment**:
   - Note any data quality issues observed
   - Suggest improvements or validation rules
   - Highlight patterns or anomalies

Focus on creating production-ready data definitions that would help developers and analysts understand and work with this data effectively.

Generate the complete data definition now:"""

        return prompt
    
    def _parse_definition_response(self,
                                 llm_response: str,
                                 file_path: str,
                                 analysis: Dict[str, Any],
                                 dataset_info: LineageNode) -> TableDefinition:
        """Parse LLM response into TableDefinition object"""
        
        try:
            # Extract JSON from LLM response
            json_start = llm_response.find('{')
            json_end = llm_response.rfind('}') + 1
            
            if json_start != -1 and json_end > json_start:
                json_str = llm_response[json_start:json_end]
                llm_data = json.loads(json_str)
                
                # Extract table definition
                table_def_data = llm_data.get('table_definition', {})
                column_def_data = llm_data.get('column_definitions', [])
                
                # Create column definitions
                columns = []
                for col_data in column_def_data:
                    columns.append(ColumnDefinition(
                        column_name=col_data.get('column_name', ''),
                        data_type=col_data.get('data_type', ''),
                        max_length=col_data.get('max_length'),
                        is_nullable=col_data.get('is_nullable', True),
                        constraints=col_data.get('constraints', []),
                        description=col_data.get('description', ''),
                        business_rules=col_data.get('business_rules', []),
                        sample_values=col_data.get('sample_values', [])
                    ))
                
                # Create table definition
                table_def = TableDefinition(
                    table_name=table_def_data.get('table_name', dataset_info.dataset_name),
                    file_path=file_path,
                    business_purpose=table_def_data.get('business_purpose', dataset_info.business_logic),
                    columns=columns,
                    primary_keys=table_def_data.get('primary_keys', []),
                    foreign_keys=table_def_data.get('foreign_keys', []),
                    indexes=table_def_data.get('indexes', []),
                    row_count=analysis['row_count'],
                    data_quality_notes=table_def_data.get('data_quality_notes', []),
                    business_context=table_def_data.get('business_context', '')
                )
                
                return table_def
                
        except Exception as e:
            print(f"Warning: Could not parse LLM response: {e}")
        
        # Fallback to basic definition
        return self._create_fallback_definition(file_path, analysis, dataset_info)
    
    def _create_fallback_definition(self,
                                  file_path: str,
                                  analysis: Dict[str, Any],
                                  dataset_info: LineageNode) -> TableDefinition:
        """Create basic definition when LLM fails"""
        
        columns = []
        for col_name in analysis['columns']:
            col_analysis = analysis['column_analysis'][col_name]
            
            # Determine basic data type
            dtype = col_analysis['dtype']
            if 'int' in dtype:
                data_type = 'INTEGER'
            elif 'float' in dtype:
                data_type = 'DECIMAL(10,2)'
            elif 'datetime' in dtype:
                data_type = 'TIMESTAMP'
            elif 'bool' in dtype:
                data_type = 'BOOLEAN'
            else:
                max_len = col_analysis.get('max_length', 255)
                data_type = f'VARCHAR({min(max_len, 255)})'
            
            columns.append(ColumnDefinition(
                column_name=col_name,
                data_type=data_type,
                max_length=col_analysis.get('max_length'),
                is_nullable=col_analysis['null_count'] > 0,
                description=f"Column from {dataset_info.dataset_name}",
                sample_values=col_analysis.get('sample_values', [])[:3]
            ))
        
        return TableDefinition(
            table_name=dataset_info.dataset_name.replace('.', '_'),
            file_path=file_path,
            business_purpose=dataset_info.business_logic,
            columns=columns,
            row_count=analysis['row_count'],
            business_context=f"Source data for {dataset_info.dataset_name}"
        )


def generate_data_definitions(source_directory: str,
                            lineage: DatasetLineage,
                            sample_size: int = 100) -> DataDefinitionContext:
    """
    Main function to generate data definitions for source tables
    
    Args:
        source_directory: Directory containing source CSV files
        lineage: Dataset lineage with source dataset information
        sample_size: Number of rows to sample for analysis
        
    Returns:
        DataDefinitionContext with generated definitions
    """
    generator = DataDefinitionGenerator()
    return generator.generate_data_definitions(source_directory, lineage, sample_size)


def print_data_definitions(context: DataDefinitionContext):
    """Print formatted summary of generated data definitions"""
    
    print("=" * 80)
    print("📋 DATA DEFINITIONS SUMMARY")
    print("=" * 80)
    
    metadata = context.generation_metadata
    print(f"\n📊 Generation Summary:")
    print(f"   Source Directory: {metadata.get('source_directory', 'N/A')}")
    print(f"   Files Found: {metadata.get('total_files_found', 0)}")
    print(f"   Files Matched: {metadata.get('files_matched', 0)}")
    print(f"   Definitions Generated: {metadata.get('definitions_generated', 0)}")
    print(f"   Sample Size Used: {metadata.get('sample_size_used', 0)}")
    
    print(f"\n📋 TABLE DEFINITIONS ({len(context.table_definitions)}):")
    print("=" * 80)
    
    for i, table_def in enumerate(context.table_definitions, 1):
        print(f"\n{i}. 📊 {table_def.table_name.upper()}")
        print(f"   📁 File: {os.path.basename(table_def.file_path)}")
        print(f"   📈 Rows: {table_def.row_count:,}")
        print(f"   📋 Columns: {len(table_def.columns)}")
        print(f"   🎯 Purpose: {table_def.business_purpose}")
        
        if table_def.primary_keys:
            print(f"   🔑 Primary Keys: {', '.join(table_def.primary_keys)}")
        
        if table_def.foreign_keys:
            print(f"   🔗 Foreign Keys: {len(table_def.foreign_keys)}")
        
        print(f"\n   📋 Column Details:")
        print("   " + "-" * 70)
        
        for col in table_def.columns[:5]:  # Show first 5 columns
            nullable = "NULL" if col.is_nullable else "NOT NULL"
            constraints_text = f" ({', '.join(col.constraints)})" if col.constraints else ""
            
            print(f"   • {col.column_name:<20} {col.data_type:<15} {nullable:<8}{constraints_text}")
            if col.description:
                print(f"     Description: {col.description}")
            if col.sample_values:
                print(f"     Sample: {', '.join(col.sample_values[:3])}")
        
        if len(table_def.columns) > 5:
            print(f"   ... and {len(table_def.columns) - 5} more columns")
        
        if table_def.data_quality_notes:
            print(f"\n   ⚠️  Data Quality Notes:")
            for note in table_def.data_quality_notes:
                print(f"      • {note}")


def export_data_definitions(context: DataDefinitionContext, output_file: str):
    """Export data definitions to JSON file"""
    
    export_data = {
        "metadata": context.generation_metadata,
        "table_definitions": []
    }
    
    for table_def in context.table_definitions:
        table_data = {
            "table_name": table_def.table_name,
            "file_path": table_def.file_path,
            "business_purpose": table_def.business_purpose,
            "business_context": table_def.business_context,
            "row_count": table_def.row_count,
            "primary_keys": table_def.primary_keys,
            "foreign_keys": table_def.foreign_keys,
            "indexes": table_def.indexes,
            "data_quality_notes": table_def.data_quality_notes,
            "columns": [
                {
                    "column_name": col.column_name,
                    "data_type": col.data_type,
                    "max_length": col.max_length,
                    "is_nullable": col.is_nullable,
                    "constraints": col.constraints,
                    "description": col.description,
                    "business_rules": col.business_rules,
                    "sample_values": col.sample_values
                } for col in table_def.columns
            ]
        }
        export_data["table_definitions"].append(table_data)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False)
    
    print(f"📤 Data definitions exported to: {output_file}")


def generate_ddl_scripts(context: DataDefinitionContext, output_dir: str):
    """Generate SQL DDL scripts for table creation"""
    
    os.makedirs(output_dir, exist_ok=True)
    
    for table_def in context.table_definitions:
        ddl_lines = [
            f"-- Data Definition for {table_def.table_name}",
            f"-- Purpose: {table_def.business_purpose}",
            f"-- Source File: {os.path.basename(table_def.file_path)}",
            f"-- Rows: {table_def.row_count:,}",
            "",
            f"CREATE TABLE {table_def.table_name} ("
        ]
        
        # Add column definitions
        col_definitions = []
        for col in table_def.columns:
            col_def = f"    {col.column_name} {col.data_type}"
            
            if not col.is_nullable:
                col_def += " NOT NULL"
            
            if col.constraints:
                for constraint in col.constraints:
                    col_def += f" {constraint}"
            
            col_definitions.append(col_def)
        
        ddl_lines.append(',\n'.join(col_definitions))
        
        # Add primary key
        if table_def.primary_keys:
            ddl_lines.append(f",\n    PRIMARY KEY ({', '.join(table_def.primary_keys)})")
        
        ddl_lines.append("\n);")
        
        # Add comments
        ddl_lines.append("")
        ddl_lines.append(f"COMMENT ON TABLE {table_def.table_name} IS '{table_def.business_purpose}';")
        
        for col in table_def.columns:
            if col.description:
                ddl_lines.append(f"COMMENT ON COLUMN {table_def.table_name}.{col.column_name} IS '{col.description}';")
        
        # Add indexes
        if table_def.indexes:
            ddl_lines.append("")
            for idx_col in table_def.indexes:
                idx_name = f"idx_{table_def.table_name}_{idx_col}"
                ddl_lines.append(f"CREATE INDEX {idx_name} ON {table_def.table_name} ({idx_col});")
        
        # Write DDL file
        ddl_file = os.path.join(output_dir, f"{table_def.table_name}.sql")
        with open(ddl_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(ddl_lines))
    
    print(f"📜 DDL scripts generated in: {output_dir}")


# Example usage
if __name__ == "__main__":
    # This would be used with actual lineage and source directory
    print("Data Definition Generator ready for use!")
    print("Usage: generate_data_definitions(source_directory, lineage)")