"""
Streamlit UI for SAS Dataset Lineage and Data Definition Generator

A comprehensive web interface for:
1. SAS script analysis and dataset extraction
2. Dataset lineage generation using LLM
3. Data definition creation for source tables
"""

import streamlit as st
import pandas as pd
import json
import os
from pathlib import Path
import traceback
from typing import Dict, List, Any, Optional

# Import our modules
try:
    from enhanced_ast_walker import extract_comprehensive_context, LineageExtractionContext
    from llm_lineage_generator import generate_lineage_with_llm, DatasetLineage, print_lineage_summary
    from data_definition_generator import generate_data_definitions, print_data_definitions, export_data_definitions
    from lineage_visualizer import LineageVisualizer, visualize_lineage
except ImportError as e:
    st.error(f"Error importing modules: {e}")
    st.stop()

# Page configuration
st.set_page_config(
    page_title="SAS Dataset Lineage & Data Definition Generator",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
def init_session_state():
    """Initialize session state variables"""
    if 'ast_context' not in st.session_state:
        st.session_state.ast_context = None
    if 'dataset_lineage' not in st.session_state:
        st.session_state.dataset_lineage = None
    if 'data_definitions' not in st.session_state:
        st.session_state.data_definitions = None
    if 'source_directory' not in st.session_state:
        st.session_state.source_directory = ""
    if 'sas_script_content' not in st.session_state:
        st.session_state.sas_script_content = ""

init_session_state()

# Helper functions
def parse_sas_script(script_content: str) -> Dict[str, Any]:
    """Parse SAS script content (mock AST for demo)"""
    # This would normally parse real SAS script
    # For demo purposes, creating a mock AST based on script content
    
    mock_ast = {
        'type': 'SasModule',
        'body': []
    }
    
    # Simple parsing to identify DATA steps and PROC SQL
    lines = script_content.split('\n')
    current_step = None
    
    for line in lines:
        line_clean = line.strip().upper()
        
        if line_clean.startswith('DATA '):
            dataset_name = line_clean.replace('DATA ', '').replace(';', '').strip()
            current_step = {
                'type': 'SasDataStep',
                'name': dataset_name,
                'datasets_out': [dataset_name],
                'statements': []
            }
            mock_ast['body'].append(current_step)
            
        elif 'SET ' in line_clean and current_step:
            dataset_ref = line_clean.split('SET ')[1].replace(';', '').strip()
            current_step['statements'].append({
                'type': 'SasSetStatement',
                'datasets': [{'type': 'SasDatasetRef', 'name': dataset_ref}]
            })
            
        elif line_clean.startswith('PROC SQL'):
            current_step = {
                'type': 'SasProcStep',
                'procedure_name': 'sql',
                'statements': []
            }
            mock_ast['body'].append(current_step)
            
        elif 'CREATE TABLE' in line_clean and current_step:
            table_name = line_clean.split('CREATE TABLE ')[1].split(' ')[0].strip()
            current_step['statements'].append({
                'type': 'SasProcSqlCreateTableStatement',
                'table_name': table_name,
                'select_statement': {'from': {'main_table': {'table_name': 'sample_table'}}}
            })
    
    return mock_ast

def display_dataset_info(context: LineageExtractionContext):
    """Display dataset information from AST context"""
    
    st.subheader("📊 Dataset Information")
    
    # Dataset Summary
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Data Steps", len(context.data_steps))
    with col2:
        st.metric("SQL Operations", len(context.sql_operations))
    with col3:
        total_datasets = len(context.dataset_summary.get('source_datasets', []) + 
                           context.dataset_summary.get('derived_datasets', []))
        st.metric("Total Datasets", total_datasets)
    
    # Dataset Categories
    st.subheader("📁 Dataset Categories")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write("**📥 Source Datasets**")
        source_datasets = context.dataset_summary.get('source_datasets', [])
        for dataset in source_datasets:
            st.write(f"• {dataset}")
    
    with col2:
        st.write("**⚙️ Derived Datasets**") 
        derived_datasets = context.dataset_summary.get('derived_datasets', [])
        for dataset in derived_datasets:
            st.write(f"• {dataset}")
    
    with col3:
        st.write("**🔗 Referenced Datasets**")
        ref_datasets = context.dataset_summary.get('all_referenced', [])
        for dataset in ref_datasets:
            st.write(f"• {dataset}")
    
    # Operation Sequence
    if context.operation_sequence:
        st.subheader("🔄 Operation Sequence")
        for i, operation in enumerate(context.operation_sequence, 1):
            st.write(f"{i}. {operation}")

def display_lineage_analysis(lineage: DatasetLineage):
    """Display comprehensive lineage analysis"""
    
    st.subheader("📊 COMPREHENSIVE DATASET LINEAGE ANALYSIS")
    st.write("=" * 80)
    
    # Flow description
    st.subheader("🔄 Data Flow Overview")
    st.info(lineage.flow_description)
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    source_nodes = [n for n in lineage.nodes if n.dataset_type == 'source']
    intermediate_nodes = [n for n in lineage.nodes if n.dataset_type == 'intermediate']
    final_nodes = [n for n in lineage.nodes if n.dataset_type == 'final']
    
    with col1:
        st.metric("Total Nodes", len(lineage.nodes))
    with col2:
        st.metric("Source Datasets", len(source_nodes))
    with col3:
        st.metric("Intermediate Datasets", len(intermediate_nodes))
    with col4:
        st.metric("Relationships", len(lineage.relationships))
    
    # Dataset Details
    st.subheader("📁 Dataset Details")
    
    # Source datasets
    if source_nodes:
        st.write("**📥 Source Datasets:**")
        for node in source_nodes:
            with st.expander(f"📥 {node.dataset_name}"):
                st.write(f"**Purpose:** {node.business_logic}")
                st.write(f"**Created by:** {node.created_by}")
    
    # Intermediate datasets
    if intermediate_nodes:
        st.write("**⚙️ Intermediate Datasets:**")
        for node in intermediate_nodes:
            with st.expander(f"⚙️ {node.dataset_name}"):
                st.write(f"**Purpose:** {node.business_logic}")
                st.write(f"**Created by:** {node.created_by}")
                st.write(f"**Inputs:** {', '.join(node.inputs)}")
                if node.variables_created:
                    st.write(f"**New Variables:** {', '.join(node.variables_created)}")
                if node.transformations:
                    st.write("**Transformations:**")
                    for transform in node.transformations:
                        st.write(f"• {transform}")
    
    # Final datasets
    if final_nodes:
        st.write("**🎯 Final Datasets:**")
        for node in final_nodes:
            with st.expander(f"🎯 {node.dataset_name}"):
                st.write(f"**Purpose:** {node.business_logic}")
                st.write(f"**Created by:** {node.created_by}")
    
    # Relationships
    st.subheader("🔗 Data Flow Relationships")
    for rel in lineage.relationships:
        with st.expander(f"{rel.source_dataset} ➜ {rel.target_dataset}"):
            st.write(f"**Type:** {rel.transformation_type}")
            st.write(f"**Description:** {rel.transformation_description}")
            if rel.variables_passed:
                st.write(f"**Variables:** {', '.join(rel.variables_passed[:3])}...")
    
    # Variable lineage
    if lineage.variable_lineage:
        st.subheader("🧬 Variable Lineage")
        for var, sources in lineage.variable_lineage.items():
            st.write(f"**{var}** ← derived from: {', '.join(sources)}")

def find_csv_files(directory: str) -> List[str]:
    """Find all CSV files in directory"""
    csv_files = []
    try:
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.lower().endswith(('.csv', '.tsv')):
                    csv_files.append(os.path.join(root, file))
    except Exception as e:
        st.error(f"Error scanning directory: {e}")
    
    return sorted(csv_files)

def match_csv_to_datasets(csv_files: List[str], source_datasets: List[str]) -> Dict[str, str]:
    """Match CSV files to source datasets"""
    matches = {}
    
    for csv_file in csv_files:
        file_name = os.path.basename(csv_file).lower()
        file_stem = Path(csv_file).stem.lower()
        
        for dataset in source_datasets:
            dataset_clean = dataset.lower().replace('raw.', '').replace('source.', '')
            
            if (dataset_clean in file_stem or 
                file_stem in dataset_clean or
                dataset_clean.replace('_', '') in file_stem.replace('_', '')):
                matches[csv_file] = dataset
                break
    
    return matches

def generate_mermaid_diagram(lineage: DatasetLineage) -> str:
    """Generate Mermaid flowchart code for dataset lineage"""
    
    lines = ["flowchart TD"]
    
    # Create node definitions with styling
    for node in lineage.nodes:
        node_id = node.dataset_name.replace('.', '_').replace(' ', '_').replace('-', '_')
        
        if node.dataset_type == 'source':
            lines.append(f'    {node_id}["{node.dataset_name}"]')
            lines.append(f'    class {node_id} sourceClass')
        elif node.dataset_type == 'final':
            lines.append(f'    {node_id}["{node.dataset_name}"]')
            lines.append(f'    class {node_id} finalClass')
        else:  # intermediate
            lines.append(f'    {node_id}["{node.dataset_name}"]')
            lines.append(f'    class {node_id} intermediateClass')
    
    # Add relationships with labels
    for rel in lineage.relationships:
        source_id = rel.source_dataset.replace('.', '_').replace(' ', '_').replace('-', '_')
        target_id = rel.target_dataset.replace('.', '_').replace(' ', '_').replace('-', '_')
        
        transform_label = rel.transformation_type.replace('_', ' ').title()
        lines.append(f'    {source_id} -->|{transform_label}| {target_id}')
    
    # Add CSS styling
    lines.extend([
        "",
        "    classDef sourceClass fill:#e8f4fd,stroke:#1e88e5,stroke-width:3px,color:#000",
        "    classDef intermediateClass fill:#fff2cc,stroke:#fb8c00,stroke-width:2px,color:#000", 
        "    classDef finalClass fill:#e8f5e8,stroke:#4caf50,stroke-width:3px,color:#000"
    ])
    
    return '\n'.join(lines)

def generate_mermaid_gantt(lineage: DatasetLineage) -> str:
    """Generate Mermaid Gantt chart showing data flow sequence"""
    
    lines = [
        "gantt",
        "    title Dataset Processing Flow",
        "    dateFormat X",
        "    axisFormat %s"
    ]
    
    # Group operations by type
    lines.append("    section Data Sources")
    source_nodes = [n for n in lineage.nodes if n.dataset_type == 'source']
    for i, node in enumerate(source_nodes, 1):
        clean_name = node.dataset_name.replace('.', ' ')
        lines.append(f"    {clean_name} : 0, {i}")
    
    lines.append("    section Data Processing")
    intermediate_nodes = [n for n in lineage.nodes if n.dataset_type == 'intermediate']
    start_time = len(source_nodes) + 1
    for i, node in enumerate(intermediate_nodes):
        clean_name = node.dataset_name.replace('.', ' ')
        lines.append(f"    {clean_name} : {start_time + i}, {start_time + i + 1}")
    
    lines.append("    section Final Outputs")
    final_nodes = [n for n in lineage.nodes if n.dataset_type == 'final']
    start_time = len(source_nodes) + len(intermediate_nodes) + 1
    for i, node in enumerate(final_nodes):
        clean_name = node.dataset_name.replace('.', ' ')
        lines.append(f"    {clean_name} : {start_time + i}, {start_time + i + 1}")
    
    return '\n'.join(lines)

# Main App
def main():
    """Main Streamlit application"""
    
    # Header
    st.title("📊 SAS Dataset Lineage & Data Definition Generator")
    st.markdown("---")
    
    # Sidebar
    with st.sidebar:
        st.header("🔧 Configuration")
        
        # Data Source Configuration
        with st.expander("📁 Data Source Configuration", expanded=True):
            source_dir = st.text_input(
                "Source Data Directory",
                value=st.session_state.source_directory,
                placeholder="/path/to/csv/files",
                help="Directory containing source CSV files"
            )
            
            sas_script = st.text_area(
                "SAS Script Content",
                value=st.session_state.sas_script_content,
                height=200,
                placeholder="Paste your SAS script here...",
                help="SAS script to analyze for dataset lineage"
            )
            
            # Save to session state
            if source_dir != st.session_state.source_directory:
                st.session_state.source_directory = source_dir
            if sas_script != st.session_state.sas_script_content:
                st.session_state.sas_script_content = sas_script
            
            # Analyze button
            if st.button("🔍 Analyze SAS Script", type="primary"):
                if sas_script.strip():
                    with st.spinner("Analyzing SAS script..."):
                        try:
                            # Parse SAS script to AST (mock for demo)
                            mock_ast = parse_sas_script(sas_script)
                            
                            # Extract context
                            context = extract_comprehensive_context(mock_ast)
                            st.session_state.ast_context = context
                            
                            st.success("✅ SAS script analyzed successfully!")
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"Error analyzing script: {e}")
                            st.error(traceback.format_exc())
                else:
                    st.warning("Please enter a SAS script to analyze")
    
    # Main content area
    if st.session_state.ast_context is None:
        st.info("👆 Please configure your data source and analyze a SAS script to begin")
        
        # Show example
        with st.expander("📝 Example SAS Script"):
            example_script = """
/* Sample SAS Script */
data customers_clean;
    set raw.customer_data;
    where status = 'ACTIVE';
    total_spend = sum(spend_cat1, spend_cat2, spend_cat3);
    if total_spend < 0 then total_spend = 0;
run;

data transactions_clean;
    set raw.transaction_data;
    where txn_status = 'SUCCESS';
    txn_month = month(txn_date);
run;

proc sql;
    create table customer_txn_summary as
    select 
        a.customer_id,
        a.total_spend,
        count(distinct b.txn_id) as txn_count,
        sum(b.amount) as txn_total
    from customers_clean a
    left join transactions_clean b
        on a.customer_id = b.customer_id
    group by a.customer_id, a.total_spend;
quit;
"""
            st.code(example_script, language='sql')
    
    else:
        # Tabs for different functionality
        tab1, tab2, tab3 = st.tabs(["📊 Dataset Analysis", "🔗 Lineage Generation", "📋 Data Definitions"])
        
        with tab1:
            st.header("📊 Dataset Analysis")
            display_dataset_info(st.session_state.ast_context)
        
        with tab2:
            st.header("🔗 Dataset Lineage Generation")
            
            if st.button("🚀 Generate Lineage", type="primary"):
                with st.spinner("Generating dataset lineage using LLM..."):
                    try:
                        lineage = generate_lineage_with_llm(st.session_state.ast_context)
                        st.session_state.dataset_lineage = lineage
                        st.success("✅ Lineage generated successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error generating lineage: {e}")
                        st.error(traceback.format_exc())
            
            if st.session_state.dataset_lineage:
                display_lineage_analysis(st.session_state.dataset_lineage)
                
                # Visualization options
                st.subheader("📈 Visualization Options")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("🎨 Generate Mermaid Flowchart"):
                        try:
                            mermaid_code = generate_mermaid_diagram(st.session_state.dataset_lineage)
                            st.subheader("🎨 Mermaid Flowchart Code")
                            st.info("Copy this code and paste it into https://mermaid.live/ to visualize")
                            st.code(mermaid_code, language='text')
                            
                            # Download button for Mermaid code
                            st.download_button(
                                label="💾 Download Mermaid Code",
                                data=mermaid_code,
                                file_name="lineage_flowchart.mmd",
                                mime="text/plain"
                            )
                        except Exception as e:
                            st.error(f"Error creating Mermaid diagram: {e}")
                
                with col2:
                    if st.button("📊 Generate Gantt Chart"):
                        try:
                            gantt_code = generate_mermaid_gantt(st.session_state.dataset_lineage)
                            st.subheader("📊 Mermaid Gantt Chart Code")
                            st.info("Copy this code and paste it into https://mermaid.live/ to visualize the processing flow")
                            st.code(gantt_code, language='text')
                            
                            # Download button for Gantt code
                            st.download_button(
                                label="💾 Download Gantt Code",
                                data=gantt_code,
                                file_name="lineage_gantt.mmd",
                                mime="text/plain"
                            )
                        except Exception as e:
                            st.error(f"Error creating Gantt chart: {e}")
                
                with col3:
                    if st.button("📤 Export Lineage JSON"):
                        try:
                            # Export to JSON
                            export_data = {
                                "metadata": {
                                    "total_nodes": len(st.session_state.dataset_lineage.nodes),
                                    "total_relationships": len(st.session_state.dataset_lineage.relationships),
                                    "flow_description": st.session_state.dataset_lineage.flow_description
                                },
                                "nodes": [
                                    {
                                        "dataset_name": n.dataset_name,
                                        "dataset_type": n.dataset_type,
                                        "business_logic": n.business_logic,
                                        "created_by": n.created_by,
                                        "inputs": n.inputs,
                                        "transformations": n.transformations,
                                        "variables_created": n.variables_created
                                    } for n in st.session_state.dataset_lineage.nodes
                                ],
                                "relationships": [
                                    {
                                        "source_dataset": r.source_dataset,
                                        "target_dataset": r.target_dataset,
                                        "transformation_type": r.transformation_type,
                                        "transformation_description": r.transformation_description,
                                        "variables_passed": r.variables_passed
                                    } for r in st.session_state.dataset_lineage.relationships
                                ],
                                "variable_lineage": st.session_state.dataset_lineage.variable_lineage
                            }
                            
                            st.download_button(
                                label="💾 Download Complete Lineage",
                                data=json.dumps(export_data, indent=2),
                                file_name="dataset_lineage.json",
                                mime="application/json"
                            )
                        except Exception as e:
                            st.error(f"Error exporting lineage: {e}")
                
                # Add instructions for Mermaid
                st.subheader("ℹ️ How to Use Mermaid Diagrams")
                st.info("""
                **Steps to visualize your diagrams:**
                1. Click the buttons above to generate Mermaid code
                2. Copy the generated code 
                3. Go to https://mermaid.live/
                4. Paste the code in the editor
                5. See your beautiful diagram!
                
                **Mermaid.live** is a free online editor for Mermaid diagrams with export options to PNG, SVG, and PDF.
                """)
        
        with tab3:
            st.header("📋 Data Definition Generation")
            
            if not st.session_state.source_directory:
                st.warning("⚠️ Please configure the source data directory first")
            elif not st.session_state.dataset_lineage:
                st.warning("⚠️ Please generate dataset lineage first")
            else:
                # Find CSV files and match to datasets
                csv_files = find_csv_files(st.session_state.source_directory)
                
                if not csv_files:
                    st.warning(f"⚠️ No CSV files found in {st.session_state.source_directory}")
                else:
                    st.info(f"📁 Found {len(csv_files)} CSV files")
                    
                    # Get source datasets from lineage
                    source_datasets = [n.dataset_name for n in st.session_state.dataset_lineage.nodes 
                                     if n.dataset_type == 'source']
                    
                    # Match files to datasets
                    matches = match_csv_to_datasets(csv_files, source_datasets)
                    
                    st.subheader("🔗 Dataset Mapping")
                    
                    if matches:
                        st.success(f"✅ Matched {len(matches)} CSV files to source datasets")
                        
                        # Show matches
                        for csv_file, dataset in matches.items():
                            with st.expander(f"📄 {os.path.basename(csv_file)} ↔ {dataset}"):
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    st.write("**File Info:**")
                                    st.write(f"Path: {csv_file}")
                                    try:
                                        file_size = os.path.getsize(csv_file)
                                        st.write(f"Size: {file_size:,} bytes")
                                    except:
                                        st.write("Size: Unknown")
                                
                                with col2:
                                    st.write("**Dataset Info:**")
                                    dataset_node = next((n for n in st.session_state.dataset_lineage.nodes 
                                                       if n.dataset_name == dataset), None)
                                    if dataset_node:
                                        st.write(f"Purpose: {dataset_node.business_logic}")
                                
                                # Show sample data
                                try:
                                    df_sample = pd.read_csv(csv_file, nrows=5)
                                    st.write("**Sample Data:**")
                                    st.dataframe(df_sample, use_container_width=True)
                                except Exception as e:
                                    st.error(f"Error reading CSV: {e}")
                        
                        # Generate data definitions button
                        st.subheader("🚀 Generate Data Definitions")
                        
                        sample_size = st.slider("Sample Size for Analysis", 50, 500, 100, 50)
                        
                        if st.button("🔄 Generate Data Definitions", type="primary"):
                            with st.spinner("Generating data definitions using LLM..."):
                                try:
                                    data_def_context = generate_data_definitions(
                                        st.session_state.source_directory,
                                        st.session_state.dataset_lineage,
                                        sample_size
                                    )
                                    st.session_state.data_definitions = data_def_context
                                    st.success("✅ Data definitions generated successfully!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error generating data definitions: {e}")
                                    st.error(traceback.format_exc())
                        
                        # Display data definitions if available
                        if st.session_state.data_definitions:
                            st.subheader("📋 Generated Data Definitions")
                            
                            for table_def in st.session_state.data_definitions.table_definitions:
                                with st.expander(f"📊 {table_def.table_name.upper()}", expanded=True):
                                    col1, col2 = st.columns(2)
                                    
                                    with col1:
                                        st.write("**Table Information:**")
                                        st.write(f"**Rows:** {table_def.row_count:,}")
                                        st.write(f"**Columns:** {len(table_def.columns)}")
                                        st.write(f"**Purpose:** {table_def.business_purpose}")
                                        
                                        if table_def.primary_keys:
                                            st.write(f"**Primary Keys:** {', '.join(table_def.primary_keys)}")
                                        
                                        if table_def.data_quality_notes:
                                            st.write("**Data Quality Notes:**")
                                            for note in table_def.data_quality_notes:
                                                st.write(f"• {note}")
                                    
                                    with col2:
                                        st.write("**Column Definitions:**")
                                        
                                        # Create columns dataframe for display
                                        col_data = []
                                        for col in table_def.columns:
                                            nullable = "NULL" if col.is_nullable else "NOT NULL"
                                            constraints = ', '.join(col.constraints) if col.constraints else ""
                                            
                                            col_data.append({
                                                "Column": col.column_name,
                                                "Data Type": col.data_type,
                                                "Nullable": nullable,
                                                "Constraints": constraints,
                                                "Description": col.description[:50] + "..." if len(col.description) > 50 else col.description
                                            })
                                        
                                        df_cols = pd.DataFrame(col_data)
                                        st.dataframe(df_cols, use_container_width=True, hide_index=True)
                            
                            # Export options
                            st.subheader("📤 Export Options")
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                if st.button("💾 Export JSON"):
                                    export_data = {
                                        "metadata": st.session_state.data_definitions.generation_metadata,
                                        "tables": [
                                            {
                                                "table_name": td.table_name,
                                                "business_purpose": td.business_purpose,
                                                "row_count": td.row_count,
                                                "columns": [
                                                    {
                                                        "name": col.column_name,
                                                        "type": col.data_type,
                                                        "nullable": col.is_nullable,
                                                        "description": col.description
                                                    } for col in td.columns
                                                ]
                                            } for td in st.session_state.data_definitions.table_definitions
                                        ]
                                    }
                                    
                                    st.download_button(
                                        label="💾 Download Data Definitions",
                                        data=json.dumps(export_data, indent=2),
                                        file_name="data_definitions.json",
                                        mime="application/json"
                                    )
                            
                            with col2:
                                if st.button("📜 Generate DDL"):
                                    ddl_scripts = []
                                    
                                    for table_def in st.session_state.data_definitions.table_definitions:
                                        ddl = [f"-- {table_def.table_name}"]
                                        ddl.append(f"CREATE TABLE {table_def.table_name} (")
                                        
                                        col_defs = []
                                        for col in table_def.columns:
                                            col_def = f"    {col.column_name} {col.data_type}"
                                            if not col.is_nullable:
                                                col_def += " NOT NULL"
                                            col_defs.append(col_def)
                                        
                                        ddl.append(',\n'.join(col_defs))
                                        ddl.append(");")
                                        ddl.append("")
                                        
                                        ddl_scripts.append('\n'.join(ddl))
                                    
                                    st.download_button(
                                        label="📜 Download DDL Scripts",
                                        data='\n'.join(ddl_scripts),
                                        file_name="create_tables.sql",
                                        mime="text/sql"
                                    )
                    
                    else:
                        st.warning("⚠️ No CSV files could be matched to source datasets")
                        st.write("**Available CSV files:**")
                        for csv_file in csv_files:
                            st.write(f"• {os.path.basename(csv_file)}")
                        
                        st.write("**Source datasets from lineage:**")
                        for dataset in source_datasets:
                            st.write(f"• {dataset}")

if __name__ == "__main__":
    main()