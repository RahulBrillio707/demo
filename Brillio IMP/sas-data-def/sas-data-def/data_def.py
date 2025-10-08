"""
Streamlit Table Documentation Generator
A comprehensive app for viewing table schemas and generating AI-powered documentation
"""

import streamlit as st
import pandas as pd
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
import time
from datetime import datetime
from langchain_openai import AzureChatOpenAI
from langchain.schema import HumanMessage, SystemMessage

# Page configuration
st.set_page_config(
    page_title="Table Documentation Generator",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state for paths
if 'tables_dir' not in st.session_state:
    st.session_state.tables_dir = "data/tables"
if 'definitions_dir' not in st.session_state:
    st.session_state.definitions_dir = "data/definitions"

# Ensure default directories exist
Path("data/tables").mkdir(parents=True, exist_ok=True)
Path("data/definitions").mkdir(parents=True, exist_ok=True)

# Custom CSS for better UI
st.markdown("""
<style>
    .stButton > button {
        background-color: #4CAF50;
        color: white;
        font-weight: bold;
        border-radius: 5px;
        border: none;
        padding: 0.5rem 1rem;
        transition: all 0.3s;
    }
    .stButton > button:hover {
        background-color: #45a049;
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    .dataframe {
        font-size: 14px;
    }
    .json-container {
        background-color: #f5f5f5;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #ddd;
        max-height: 600px;
        overflow-y: auto;
    }
    .success-message {
        background-color: #d4edda;
        color: #155724;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #c3e6cb;
        margin: 1rem 0;
    }
    .error-message {
        background-color: #f8d7da;
        color: #721c24;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #f5c6cb;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #d1ecf1;
        color: #0c5460;
        padding: 1rem;
        border-radius: 5px;
        border: 1px solid #bee5eb;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


def get_llm_client() -> AzureChatOpenAI:
    """Get configured Azure OpenAI LLM client"""
    return AzureChatOpenAI(
        azure_deployment="gpt-4o",
        model_name="gpt-4o",
        api_version="2024-12-01-preview",
        azure_endpoint="https://tesco-code-converter.openai.azure.com/",
        temperature=0,
        max_tokens=None,
        timeout=None,
        max_retries=2,
        api_key="E0mJj5AOZjIh3gNU6uYqlB7ELBOwLcuGLT5DQfwZv77zN7zPsZzNJQQJ99BIACYeBjFXJ3w3AAABACOGInZ6"
    )


def list_available_tables(custom_path: Optional[str] = None) -> List[str]:
    """List all CSV and Parquet files in the tables directory"""
    tables_dir = Path(custom_path) if custom_path else Path(st.session_state.tables_dir)
    
    # Check if directory exists
    if not tables_dir.exists():
        return []
    
    tables = []
    try:
        for file_path in tables_dir.iterdir():
            if file_path.is_file() and file_path.suffix.lower() in ['.csv', '.parquet']:
                tables.append(file_path.name)
    except PermissionError:
        st.error(f"Permission denied accessing: {tables_dir}")
        return []
    except Exception as e:
        st.error(f"Error accessing directory: {str(e)}")
        return []
    
    return sorted(tables)


def load_table_data(table_name: str, custom_path: Optional[str] = None) -> Optional[pd.DataFrame]:
    """Load table data from CSV or Parquet file"""
    tables_dir = Path(custom_path) if custom_path else Path(st.session_state.tables_dir)
    file_path = tables_dir / table_name
    
    try:
        if file_path.suffix.lower() == '.csv':
            # Try different encodings
            for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
                try:
                    return pd.read_csv(file_path, encoding=encoding)
                except UnicodeDecodeError:
                    continue
            st.error(f"Could not decode CSV file with any standard encoding")
            return None
        elif file_path.suffix.lower() == '.parquet':
            return pd.read_parquet(file_path)
        else:
            st.error(f"Unsupported file type: {file_path.suffix}")
            return None
    except Exception as e:
        st.error(f"Error loading table: {str(e)}")
        return None


def extract_schema_and_sample(df: pd.DataFrame) -> Dict[str, Any]:
    """Extract schema information and sample data from DataFrame"""
    schema_info = {
        "columns": [],
        "sample_data": [],
        "row_count": len(df),
        "column_count": len(df.columns)
    }
    
    # Extract column information
    for col in df.columns:
        col_info = {
            "name": col,
            "dtype": str(df[col].dtype),
            "null_count": df[col].isnull().sum(),
            "unique_count": df[col].nunique(),
            "sample_values": df[col].dropna().head(3).tolist() if not df[col].empty else []
        }
        schema_info["columns"].append(col_info)
    
    # Get sample rows (first 5)
    sample_rows = df.head(5).to_dict('records')
    schema_info["sample_data"] = sample_rows
    
    return schema_info


def check_existing_definition(table_name: str, custom_def_path: Optional[str] = None) -> Optional[Dict]:
    """Check if table definition already exists and load it"""
    table_base_name = Path(table_name).stem
    definitions_dir = Path(custom_def_path) if custom_def_path else Path(st.session_state.definitions_dir)
    definition_file = definitions_dir / f"{table_base_name}.json"
    
    if definition_file.exists():
        try:
            with open(definition_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            st.warning(f"Error loading existing definition: {str(e)}")
            return None
    return None


def generate_llm_definition(table_name: str, schema_info: Dict) -> Optional[Dict]:
    """Generate table definition using LLM"""
    
    # Prepare the prompt
    system_prompt = """You are a data documentation expert. Your task is to analyze table schemas and generate comprehensive documentation.
    
    You must respond with ONLY valid JSON that strictly follows this schema:
    {
        "table_name": "<table name>",
        "table_description": "<one-paragraph description of what this table stores>",
        "columns": {
            "ColumnName": {
                "description": "<clear explanation of what this column represents>",
                "data_type": "<datatype such as string, integer, date, boolean etc.>",
                "constraints": "<any key constraints, uniqueness, nullability, primary key, foreign key etc. If unknown, write 'Not explicitly clear'>",
                "reasoning": "<why you gave this description, based on column name, sample data, or type>"
            }
        }
    }
    
    Important:
    - Include EVERY column from the schema
    - Include ALL 4 fields (description, data_type, constraints, reasoning) for each column
    - Output ONLY valid JSON, no additional text or markdown
    - Make intelligent inferences based on column names and sample data"""
    
    # Format column information
    columns_text = "Table Columns:\n"
    for col_info in schema_info["columns"]:
        columns_text += f"\n- {col_info['name']}:"
        columns_text += f"\n  Data Type: {col_info['dtype']}"
        columns_text += f"\n  Null Count: {col_info['null_count']}"
        columns_text += f"\n  Unique Values: {col_info['unique_count']}"
        if col_info['sample_values']:
            columns_text += f"\n  Sample Values: {col_info['sample_values'][:3]}"
    
    # Format sample data
    sample_text = "\n\nSample Data (first 5 rows):\n"
    if schema_info["sample_data"]:
        sample_df = pd.DataFrame(schema_info["sample_data"])
        sample_text += sample_df.to_string(max_rows=5)
    
    user_prompt = f"""Generate comprehensive documentation for this table:

Table Name: {table_name}
Total Rows: {schema_info['row_count']}
Total Columns: {schema_info['column_count']}

{columns_text}

{sample_text}

Remember to output ONLY valid JSON following the exact schema provided."""
    
    try:
        llm = get_llm_client()
        
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt)
        ]
        
        response = llm.invoke(messages)
        
        # Extract JSON from response
        response_text = response.content.strip()
        
        # Remove markdown code blocks if present
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.startswith("```"):
            response_text = response_text[3:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        
        response_text = response_text.strip()
        
        # Parse JSON
        definition = json.loads(response_text)
        
        # Validate that all columns are present
        table_columns = set(col["name"] for col in schema_info["columns"])
        definition_columns = set(definition.get("columns", {}).keys())
        
        if table_columns != definition_columns:
            missing = table_columns - definition_columns
            extra = definition_columns - table_columns
            
            if missing:
                st.warning(f"Missing columns in definition: {missing}")
            if extra:
                st.warning(f"Extra columns in definition: {extra}")
        
        return definition
        
    except json.JSONDecodeError as e:
        st.error(f"Failed to parse LLM response as JSON: {str(e)}")
        st.text("Raw LLM response:")
        st.code(response_text if 'response_text' in locals() else "No response received")
        return None
    except Exception as e:
        st.error(f"Error generating definition: {str(e)}")
        return None


def save_definition(table_name: str, definition: Dict, custom_def_path: Optional[str] = None) -> bool:
    """Save definition to JSON file"""
    try:
        table_base_name = Path(table_name).stem
        definitions_dir = Path(custom_def_path) if custom_def_path else Path(st.session_state.definitions_dir)
        
        # Ensure the definitions directory exists
        definitions_dir.mkdir(parents=True, exist_ok=True)
        
        definition_file = definitions_dir / f"{table_base_name}.json"
        
        # Add metadata
        definition["metadata"] = {
            "generated_at": datetime.now().isoformat(),
            "source_table": table_name
        }
        
        with open(definition_file, 'w', encoding='utf-8') as f:
            json.dump(definition, f, indent=2, ensure_ascii=False)
        
        return True
    except Exception as e:
        st.error(f"Error saving definition: {str(e)}")
        return False


def display_schema_info(df: pd.DataFrame):
    """Display table schema information"""
    st.subheader("📋 Table Schema")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Rows", f"{len(df):,}")
    with col2:
        st.metric("Total Columns", len(df.columns))
    with col3:
        st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Display column information
    st.markdown("### Column Information")
    
    schema_data = []
    for col in df.columns:
        schema_data.append({
            "Column Name": col,
            "Data Type": str(df[col].dtype),
            "Non-Null Count": f"{df[col].notna().sum():,}",
            "Null Count": f"{df[col].isna().sum():,}",
            "Unique Values": f"{df[col].nunique():,}",
            "Null %": f"{(df[col].isna().sum() / len(df) * 100):.1f}%"
        })
    
    schema_df = pd.DataFrame(schema_data)
    st.dataframe(schema_df, use_container_width=True, height=400)


def display_sample_data(df: pd.DataFrame):
    """Display sample data from table"""
    st.subheader("🔍 Sample Data (First 5 Rows)")
    
    # Display interactive dataframe
    st.dataframe(
        df.head(5),
        use_container_width=True,
        hide_index=False
    )

def display_definition(definition: Dict):
    """Display table definition as editable table and allow export as Markdown"""
    st.subheader("📖 Table Documentation")

    table_name = definition.get("table_name", "Unknown Table")
    st.markdown(f"### 📄 Documenting: `{table_name}`")

    # Convert column metadata into editable DataFrame
    columns_data = []
    for col_name, col_meta in definition.get("columns", {}).items():
        columns_data.append({
            "Column Name": col_name,
            "Data Type": col_meta.get("data_type", ""),
            "Description": col_meta.get("description", ""),
            "Constraints": col_meta.get("constraints", ""),
            "Reasoning": col_meta.get("reasoning", "")
        })

    editable_df = pd.DataFrame(columns_data)

    st.markdown("### ✏️ Edit Metadata")
    edited_df = st.data_editor(
        editable_df,
        use_container_width=True,
        height=400,
        num_rows="fixed",  # disallow row addition/deletion
        column_config={
            "Column Name": st.column_config.Column(disabled=True),
        }
    )

    # Save button (optional if user wants to persist back to dict)
    if st.button("💾 Save Edits to Memory", type="secondary"):
        definition["columns"] = {
            row["Column Name"]: {
                "data_type": row["Data Type"],
                "description": row["Description"],
                "constraints": row["Constraints"],
                "reasoning": row["Reasoning"]
            }
            for _, row in edited_df.iterrows()
        }
        st.success("✔️ Edits updated in memory")

    # Markdown export content
    md_content = f"# {table_name} Documentation\n\n"
    md_content += f"## Description\n{definition.get('table_description', 'No description')}\n\n"
    md_content += "## Columns\n\n"

    for _, row in edited_df.iterrows():
        md_content += f"### {row['Column Name']}\n"
        md_content += f"- **Type**: {row['Data Type']}\n"
        md_content += f"- **Description**: {row['Description']}\n"
        md_content += f"- **Constraints**: {row['Constraints']}\n"
        md_content += f"- **Reasoning**: {row['Reasoning']}\n\n"

    # Export download button
    st.markdown("### 📥 Download")
    st.download_button(
        label="📥 Download Edited Documentation (Markdown)",
        data=md_content,
        file_name=f"{table_name}_metadata.md",
        mime="text/markdown"
    )


def main():
    """Main Streamlit application"""
    
    # Header
    st.title("📊 Table Documentation Generator")
    st.markdown("Generate AI-powered documentation for your data tables")
    
    # Sidebar for table selection
    with st.sidebar:
        st.header("🗂️ Table Selection")
        
        # Path configuration section
        st.markdown("### 📂 Path Configuration")
        
        # Toggle between default and custom path
        use_custom_path = st.checkbox("Use Custom Path", value=False, 
                                     help="Enable to specify a custom directory for tables")
        
        if use_custom_path:
            # Custom path input
            custom_tables_path = st.text_input(
                "Tables Directory Path:",
                value=st.session_state.tables_dir,
                placeholder="e.g., data/tables or /path/to/your/tables",
                help="Enter the path to your tables directory"
            )
            
            custom_definitions_path = st.text_input(
                "Definitions Directory Path:",
                value=st.session_state.definitions_dir,
                placeholder="e.g., data/definitions or /path/to/definitions",
                help="Enter the path where definitions will be saved"
            )
            
            # Apply button for custom paths
            if st.button("Apply Custom Path", type="primary"):
                # Validate paths
                tables_path = Path(custom_tables_path)
                
                if tables_path.exists() and tables_path.is_dir():
                    st.session_state.tables_dir = custom_tables_path
                    st.session_state.definitions_dir = custom_definitions_path
                    st.success(f"✅ Using custom path: {custom_tables_path}")
                    st.rerun()
                else:
                    st.error(f"❌ Directory not found or invalid: {custom_tables_path}")
        else:
            # Use default paths
            st.session_state.tables_dir = "data/tables"
            st.session_state.definitions_dir = "data/definitions"
            
        # Show current active path
        st.markdown("**Active Tables Path:**")
        st.code(st.session_state.tables_dir, language=None)
        
        st.markdown("---")
        
        # List available tables
        tables = list_available_tables(st.session_state.tables_dir)
        
        if not tables:
            st.warning(f"No tables found in {st.session_state.tables_dir}")
            st.info("Please add CSV or Parquet files to the specified directory")
            
            # Provide option to create sample data
            if st.button("📝 Create Sample Table"):
                sample_dir = Path(st.session_state.tables_dir)
                sample_dir.mkdir(parents=True, exist_ok=True)
                
                # Create a sample CSV
                sample_data = {
                    'ID': [1, 2, 3, 4, 5],
                    'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Emma'],
                    'Age': [25, 30, 35, 28, 32],
                    'Department': ['Sales', 'IT', 'HR', 'Sales', 'IT'],
                    'Salary': [50000, 75000, 60000, 55000, 80000]
                }
                sample_df = pd.DataFrame(sample_data)
                sample_file = sample_dir / "sample_employees.csv"
                sample_df.to_csv(sample_file, index=False)
                st.success(f"✅ Created sample file: {sample_file}")
                st.rerun()
            return
        
        selected_table = st.selectbox(
            "Select a table:",
            options=tables,
            index=0,
            help="Choose a table to view and document"
        )
        
        st.markdown("---")
        st.markdown("### 📁 Available Tables")
        for table in tables:
            if table == selected_table:
                st.success(f"✓ {table}")
            else:
                st.text(f"  {table}")
        
        st.markdown("---")
        
        # File upload option
        st.markdown("### 📤 Upload Table")
        uploaded_file = st.file_uploader(
            "Or upload a CSV/Parquet file",
            type=['csv', 'parquet'],
            help="Upload a file to analyze it directly"
        )
        
        if uploaded_file is not None:
            # Save uploaded file to the current tables directory
            upload_path = Path(st.session_state.tables_dir)
            upload_path.mkdir(parents=True, exist_ok=True)
            
            file_path = upload_path / uploaded_file.name
            with open(file_path, 'wb') as f:
                f.write(uploaded_file.getbuffer())
            
            st.success(f"✅ File uploaded: {uploaded_file.name}")
            st.info(f"Saved to: {file_path}")
            time.sleep(1)
            st.rerun()
        
        st.markdown("---")
        st.markdown("### ℹ️ About")
        st.info(
            "This app helps you:\n"
            "• View table schemas\n"
            "• Preview sample data\n"
            "• Generate AI documentation\n"
            "• Save definitions for reuse\n"
            "• Use custom directories\n"
            "• Upload files directly"
        )
    
    # Main content area
    if selected_table:
        # Load table data using session state path
        df = load_table_data(selected_table, st.session_state.tables_dir)
        
        if df is not None:
            # Create tabs for different sections
            tab1, tab2, tab3 = st.tabs(["📋 Schema & Data", "📖 Documentation", "⚙️ Settings"])
            
            with tab1:
                # Display schema information
                display_schema_info(df)
                
                st.markdown("---")
                
                # Display sample data
                display_sample_data(df)
            
            with tab2:
                # Check for existing definition using session state path
                existing_definition = check_existing_definition(selected_table, st.session_state.definitions_dir)
                
                if existing_definition:
                    st.success("✅ Documentation exists for this table")
                    display_definition(existing_definition)
                    
                    # Option to regenerate
                    st.markdown("---")
                    if st.button("🔄 Regenerate Documentation", type="secondary"):
                        with st.spinner("Generating new documentation..."):
                            schema_info = extract_schema_and_sample(df)
                            new_definition = generate_llm_definition(selected_table, schema_info)
                            
                            if new_definition:
                                if save_definition(selected_table, new_definition, st.session_state.definitions_dir):
                                    st.success("✅ Documentation regenerated and saved successfully!")
                                    st.balloons()
                                    time.sleep(1)
                                    st.rerun()
                else:
                    st.info("📝 No documentation found for this table")
                    
                    if st.button("🤖 Generate Documentation", type="primary"):
                        with st.spinner("🔮 Analyzing table and generating documentation..."):
                            # Extract schema and sample data
                            schema_info = extract_schema_and_sample(df)
                            
                            # Generate definition using LLM
                            definition = generate_llm_definition(selected_table, schema_info)
                            
                            if definition:
                                # Save definition using session state path
                                if save_definition(selected_table, definition, st.session_state.definitions_dir):
                                    st.success("✅ Documentation generated and saved successfully!")
                                    st.balloons()
                                    
                                    # Display the generated definition
                                    display_definition(definition)
                                    
                                    # Refresh to show in existing section
                                    time.sleep(2)
                                    st.rerun()
                                else:
                                    st.error("Failed to save documentation")
                                    # Still display the definition even if save failed
                                    display_definition(definition)
            
            with tab3:
                st.subheader("⚙️ Settings & Information")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("### 📂 Directory Configuration")
                    st.text_input("Tables Directory", value=str(st.session_state.tables_dir), disabled=True)
                    st.text_input("Definitions Directory", value=str(st.session_state.definitions_dir), disabled=True)
                
                with col2:
                    st.markdown("### 🤖 LLM Configuration")
                    st.text_input("Model", value="gpt-4o", disabled=True)
                    st.text_input("Temperature", value="0", disabled=True)
                
                st.markdown("---")
                
                # Export functionality
                st.markdown("### 📥 Export Options")
                
                if existing_definition or 'definition' in locals():
                    export_def = existing_definition if existing_definition else definition
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        # Download JSON
                        json_str = json.dumps(export_def, indent=2)
                        st.download_button(
                            label="📥 Download Documentation (JSON)",
                            data=json_str,
                            file_name=f"{Path(selected_table).stem}_documentation.json",
                            mime="application/json"
                        )
                    
                    with col2:
                        # Download as markdown
                        md_content = f"# {export_def.get('table_name', selected_table)} Documentation\n\n"
                        md_content += f"## Description\n{export_def.get('table_description', 'No description')}\n\n"
                        md_content += "## Columns\n\n"
                        
                        for col_name, col_info in export_def.get('columns', {}).items():
                            md_content += f"### {col_name}\n"
                            md_content += f"- **Type**: {col_info.get('data_type', 'Unknown')}\n"
                            md_content += f"- **Description**: {col_info.get('description', 'No description')}\n"
                            md_content += f"- **Constraints**: {col_info.get('constraints', 'Not explicitly clear')}\n"
                            md_content += f"- **Reasoning**: {col_info.get('reasoning', 'No reasoning')}\n\n"
                        
                        st.download_button(
                            label="📥 Download Documentation (Markdown)",
                            data=md_content,
                            file_name=f"{Path(selected_table).stem}_documentation.md",
                            mime="text/markdown"
                        )


if __name__ == "__main__":
    main()