import streamlit as st
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import os
import glob
from pathlib import Path

# Import the workflows
from analysis_workflow import analysis_pipeline, SASAnalysisState
from conversion_workflow import pipeline as conversion_pipeline, SASPipelineState
from enhanced_batch_processing import EnhancedBatchProcessor

# Configure the page
st.set_page_config(
    page_title="SAS Code Analyzer & Translator",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .section-header {
        font-size: 1.5rem;
        color: #2E4057;
        border-bottom: 2px solid #1f77b4;
        padding-bottom: 0.5rem;
        margin: 1.5rem 0 1rem 0;
    }
    .status-box {
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .status-running {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
    }
    .status-completed {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
    }
    .status-error {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border: 1px solid #dee2e6;
        margin: 0.5rem 0;
    }
    .code-container {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
        margin: 1rem 0;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 3rem;
        padding: 0.5rem 1.5rem;
        background-color: #f0f2f6;
        border-radius: 0.5rem 0.5rem 0 0;
        color: #262730;
        font-weight: 500;
    }
    .stTabs [aria-selected="true"] {
        background-color: #1f77b4;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'analysis_state' not in st.session_state:
    st.session_state.analysis_state = None
if 'conversion_state' not in st.session_state:
    st.session_state.conversion_state = None
if 'analysis_running' not in st.session_state:
    st.session_state.analysis_running = False
if 'conversion_running' not in st.session_state:
    st.session_state.conversion_running = False
if 'batch_results' not in st.session_state:
    st.session_state.batch_results = None
if 'batch_running' not in st.session_state:
    st.session_state.batch_running = False

def run_analysis_workflow_simple(script_content):
    """Run the analysis workflow"""
    try:
        init_state = SASAnalysisState(
            script_content=script_content,
            chunk_size=100
        )
        
        # Use invoke instead of stream for simplicity
        raw_state = analysis_pipeline.invoke(init_state, config={"configurable": {"thread_id": "analysis_1"}})
        final_state=SASAnalysisState(**raw_state)
        return final_state
        
    except Exception as e:
        st.error(f"Analysis Error: {str(e)}")
        return None

def run_conversion_workflow_simple(script_content):
    """Run the conversion workflow"""
    try:
        init_state = SASPipelineState(
            script_content=script_content,
            chunk_size=100
        )
        
        # Use invoke instead of stream for simplicity
        raw_state = conversion_pipeline.invoke(init_state, config={"configurable": {"thread_id": "conversion_1"}})
        final_state= SASPipelineState(**raw_state)
        return final_state
        
    except Exception as e:
        st.error(f"Conversion Error: {str(e)}")
        return None

def run_both_workflows_parallel(script_content):
    """Run both workflows in parallel"""
    def run_analysis():
        return run_analysis_workflow_simple(script_content)
    
    def run_conversion():
        return run_conversion_workflow_simple(script_content)
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Submit both tasks
        analysis_future = executor.submit(run_analysis)
        conversion_future = executor.submit(run_conversion)
        
        # Get results
        analysis_result = analysis_future.result()
        conversion_result = conversion_future.result()
        
        return analysis_result, conversion_result

def process_single_file_batch(file_path, output_dir):
    """Process a single SAS file and save the result"""
    try:
        # Read the SAS file
        with open(file_path, 'r', encoding='utf-8') as f:
            script_content = f.read()
        
        # Run conversion workflow
        result = run_conversion_workflow_simple(script_content)
        
        if result and hasattr(result, 'merged_pyspark_code'):
            # Create output filename
            input_filename = Path(file_path).stem
            output_filename = f"{input_filename}_converted.py"
            output_path = os.path.join(output_dir, output_filename)
            
            # Save the converted PySpark code
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(result.merged_pyspark_code)
            
            return {
                'input_file': file_path,
                'output_file': output_path,
                'status': 'success',
                'python_valid': result.python_validation.get('syntax_valid', False) if hasattr(result, 'python_validation') else False,
                'pyspark_valid': result.pyspark_validation.get('basic_syntax_valid', False) if hasattr(result, 'pyspark_validation') else False,
                'chunks_processed': result.total_chunks_processed if hasattr(result, 'total_chunks_processed') else 0
            }
        else:
            return {
                'input_file': file_path,
                'output_file': None,
                'status': 'failed',
                'error': 'Conversion workflow returned no result'
            }
            
    except Exception as e:
        return {
            'input_file': file_path,
            'output_file': None,
            'status': 'error',
            'error': str(e)
        }

def run_batch_processing(input_dir, output_dir, max_workers=4, rate_limit_delay=0.5, enable_retries=True):
    """Process multiple SAS files in parallel using enhanced batch processor"""
    try:
        # Create enhanced processor with better error handling
        processor = EnhancedBatchProcessor(
            max_workers=min(max_workers, 6),  # Limit max workers to prevent overwhelming
            rate_limit_delay=rate_limit_delay, # User-configurable delay
            max_retries=2 if enable_retries else 1  # Configurable retries
        )
        
        # Run enhanced batch processing
        results = processor.run_batch_processing_enhanced(input_dir, output_dir)
        
        # Convert results to expected format for UI compatibility
        converted_results = []
        for result in results:
            converted_result = {
                'input_file': result['input_file'],
                'output_file': result.get('output_file'),
                'status': result['status'],
                'python_valid': result.get('python_valid', False),
                'pyspark_valid': result.get('pyspark_valid', False),
                'chunks_processed': result.get('chunks_processed', 0),
                'error': result.get('error')
            }
            converted_results.append(converted_result)
        
        return converted_results
        
    except Exception as e:
        st.error(f"Enhanced batch processing failed: {str(e)}")
        return []

def render_mermaid_diagram(mermaid_code, height=400):
    """Render a Mermaid diagram with multiple fallback options"""
    
    # Create columns for rendering options
    col1, col2 = st.columns([3, 1])
    
    with col2:
        render_method = st.selectbox(
            "Render Method:",
            ["Auto", "Streamlit-Mermaid", "HTML v10", "HTML v8", "Code Only"],
            key=f"render_method_{hash(mermaid_code)}"
        )
    
    with col1:
        if render_method == "Code Only":
            st.info("📊 Raw Mermaid Code (Copy to [Mermaid Live Editor](https://mermaid.live)):")
            st.code(mermaid_code, language="text")
            return
        
        # Try different rendering methods
        try:
            if render_method == "Streamlit-Mermaid" or render_method == "Auto":
                try:
                    import streamlit_mermaid as stmermaid
                    stmermaid.st_mermaid(mermaid_code, height=height)
                    return
                except ImportError:
                    if render_method == "Streamlit-Mermaid":
                        st.warning("streamlit-mermaid not installed. Install with: `pip install streamlit-mermaid`")
                        st.code(mermaid_code, language="text")
                        return
            
            # HTML rendering methods
            import streamlit.components.v1 as components
            
            if render_method == "HTML v10" or render_method == "Auto":
                # Clean code for better compatibility
                cleaned_code = mermaid_code.strip().replace('\"', "'").replace('\n', '\\n')
                
                html_string = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <script src="https://cdn.jsdelivr.net/npm/mermaid@10.6.1/dist/mermaid.min.js"></script>
                    <style>
                        .mermaid-container {{
                            background: white;
                            padding: 20px;
                            border-radius: 10px;
                            width: 100%;
                            height: {height-40}px;
                            overflow: auto;
                            border: 1px solid #ddd;
                        }}
                    </style>
                </head>
                <body>
                    <div class="mermaid-container">
                        <div id="mermaid-output"></div>
                        <script>
                            try {{
                                mermaid.initialize({{
                                    startOnLoad: false,
                                    theme: 'default',
                                    securityLevel: 'loose',
                                    flowchart: {{
                                        useMaxWidth: true,
                                        htmlLabels: false
                                    }}
                                }});
                                
                                const code = `{mermaid_code}`;
                                mermaid.render('temp-id', code)
                                    .then(result => {{
                                        document.getElementById('mermaid-output').innerHTML = result.svg;
                                    }})
                                    .catch(error => {{
                                        document.getElementById('mermaid-output').innerHTML = 
                                            `<div style="color: red; padding: 20px;">
                                                <h3>Rendering Error:</h3>
                                                <p>${{error.message}}</p>
                                                <details>
                                                    <summary>Show Code</summary>
                                                    <pre style="background: #f5f5f5; padding: 10px; margin: 10px;">${{code}}</pre>
                                                </details>
                                            </div>`;
                                    }});
                            }} catch (e) {{
                                document.getElementById('mermaid-output').innerHTML = 
                                    `<div style="color: red;">JavaScript Error: ${{e.message}}</div>`;
                            }}
                        </script>
                    </div>
                </body>
                </html>
                """
                components.html(html_string, height=height, scrolling=True)
                
            elif render_method == "HTML v8":
                # Fallback to older Mermaid version
                html_string = f"""
                <div style="width: 100%; height: {height}px; background: white; padding: 20px; border: 1px solid #ddd; border-radius: 10px;">
                    <script src="https://unpkg.com/mermaid@8.14.0/dist/mermaid.min.js"></script>
                    <script>
                        mermaid.initialize({{
                            startOnLoad: true,
                            theme: 'default',
                            securityLevel: 'loose'
                        }});
                    </script>
                    <div class="mermaid">
{mermaid_code}
                    </div>
                </div>
                """
                components.html(html_string, height=height)
                
        except Exception as e:
            st.error(f"Rendering failed: {str(e)}")
            st.info("📊 Fallback - Raw Mermaid Code:")
            st.code(mermaid_code, language="text")
            st.markdown("🔗 **[Open in Mermaid Live Editor](https://mermaid.live)** to view the diagram")

def display_analysis_results(analysis_state):
    """Display analysis workflow results"""
    if not analysis_state:
        st.warning("No analysis state available")
        return
    
    # Debug info
    st.write("**Debug - Analysis State Type:**", type(analysis_state).__name__)
    
    # Check if state has expected attributes
    expected_attrs = ['script_metadata', 'block_registry', 'semantic_summary', 'visualizations']
    available_attrs = [attr for attr in expected_attrs if hasattr(analysis_state, attr)]
    st.write("**Available attributes:**", available_attrs)
    
    # Script Metadata
    if hasattr(analysis_state, 'script_metadata') and analysis_state.script_metadata:
        st.markdown('<div class="section-header">📊 Script Metadata</div>', unsafe_allow_html=True)
        
        col1, col2, col3, col4 = st.columns(4)
        
        metadata = analysis_state.script_metadata
        with col1:
            st.metric("Script Name", metadata.get('script_name', 'N/A'))
        with col2:
            st.metric("Blocks Count", metadata.get('num_blocks', 0))
        with col3:
            st.metric("Macros Defined", metadata.get('macros_defined', 0))
        with col4:
            st.metric("Macros Invoked", metadata.get('macros_invoked', 0))
    
    # Block Registry
    if hasattr(analysis_state, 'block_registry') and analysis_state.block_registry:
        st.markdown('<div class="section-header">🔧 Block Registry</div>', unsafe_allow_html=True)
        
        # Display blocks in a nice format
        for i, block in enumerate(analysis_state.block_registry):
            with st.expander(f"Block {block.get('block_id', i)}: {block.get('type', 'Unknown')} - {block.get('name', 'Unnamed')}", expanded=False):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Inputs:**", block.get('inputs', []))
                    st.write("**Type:**", block.get('type', 'Unknown'))
                
                with col2:
                    st.write("**Outputs:**", block.get('outputs', []))
                    if 'params' in block:
                        st.write("**Parameters:**", block.get('params', []))
    
    # Semantic Summary
    if hasattr(analysis_state, 'semantic_summary') and analysis_state.semantic_summary:
        st.markdown('<div class="section-header">💡 Semantic Summary</div>', unsafe_allow_html=True)
        st.info(analysis_state.semantic_summary)
    
    # Visualizations
    if hasattr(analysis_state, 'visualizations') and analysis_state.visualizations:
        st.markdown('<div class="section-header">📈 Visualizations</div>', unsafe_allow_html=True)
        
        viz_tabs = st.tabs(["Block Flow", "Data Transformation", "Variable Lineage"])
        
        visualizations = analysis_state.visualizations
        
        with viz_tabs[0]:
            if 'block_flow_diagram' in visualizations:
                diagram = visualizations['block_flow_diagram']
                st.write(f"**{diagram['description']}**")
                
                # Add option to show raw code first for debugging
                if st.checkbox("🐛 Debug: Show Raw Mermaid Code", key="debug_block_flow"):
                    st.code(diagram['mermaid_code'], language="text")
                
                render_mermaid_diagram(diagram['mermaid_code'], height=800)
                
                with st.expander("📋 Copy Mermaid Code"):
                    st.code(diagram['mermaid_code'], language="mermaid")
                    if st.button("📋 Copy to Clipboard", key="copy_block_flow"):
                        st.write("✅ Copy the code above and paste it in [Mermaid Live Editor](https://mermaid.live)")
        
        with viz_tabs[1]:
            if 'data_transformation_diagram' in visualizations:
                diagram = visualizations['data_transformation_diagram']
                st.write(f"**{diagram['description']}**")
                render_mermaid_diagram(diagram['mermaid_code'], height=600)
                
                with st.expander("📋 Copy Mermaid Code"):
                    st.code(diagram['mermaid_code'], language="mermaid")
        
        with viz_tabs[2]:
            if 'variable_lineage_diagram' in visualizations:
                diagram = visualizations['variable_lineage_diagram']
                st.write(f"**{diagram['description']}**")
                render_mermaid_diagram(diagram['mermaid_code'], height=700)
                
                with st.expander("📋 Copy Mermaid Code"):
                    st.code(diagram['mermaid_code'], language="mermaid")

def display_conversion_results(conversion_state):
    """Display conversion workflow results"""
    if not conversion_state:
        st.warning("No conversion state available")
        return
    
    # Debug info
    st.write("**Debug - Conversion State Type:**", type(conversion_state).__name__)
    
    # Check if state has expected attributes
    expected_attrs = ['merged_python_code', 'merged_pyspark_code', 'python_validation', 'pyspark_validation']
    available_attrs = [attr for attr in expected_attrs if hasattr(conversion_state, attr)]
    st.write("**Available attributes:**", available_attrs)
    
    # Conversion Statistics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Chunks", getattr(conversion_state, 'total_chunks_processed', 0))
    with col2:
        st.metric("Python Blocks", getattr(conversion_state, 'python_blocks_count', 0))
    with col3:
        st.metric("PySpark Blocks", getattr(conversion_state, 'pyspark_blocks_count', 0))
    with col4:
        st.metric("SAS Blocks", getattr(conversion_state, 'sas_blocks_count', 0))
    
    # Validation Results
    if hasattr(conversion_state, 'python_validation') and conversion_state.python_validation:
        col1, col2 = st.columns(2)
        
        with col1:
            python_valid = conversion_state.python_validation.get('syntax_valid', False)
            st.metric("Python Syntax", "✅ Valid" if python_valid else "❌ Invalid")
        
        with col2:
            if hasattr(conversion_state, 'pyspark_validation') and conversion_state.pyspark_validation:
                pyspark_valid = conversion_state.pyspark_validation.get('basic_syntax_valid', False)
                st.metric("PySpark Syntax", "✅ Valid" if pyspark_valid else "❌ Invalid")
    
    # Code Outputs
    if hasattr(conversion_state, 'merged_python_code') and hasattr(conversion_state, 'merged_pyspark_code'):
        code_tabs = st.tabs(["🐍 Python Code", "⚡ PySpark Code"])
        
        with code_tabs[0]:
            if conversion_state.merged_python_code:
                st.code(conversion_state.merged_python_code, language="python")
                
                # Copy button
                if st.button("📋 Copy Python Code", key="copy_python"):
                    st.success("Python code copied to clipboard! (Use Ctrl+C)")
        
        with code_tabs[1]:
            if conversion_state.merged_pyspark_code:
                st.code(conversion_state.merged_pyspark_code, language="python")
                
                # Copy button
                if st.button("📋 Copy PySpark Code", key="copy_pyspark"):
                    st.success("PySpark code copied to clipboard! (Use Ctrl+C)")

def display_batch_results(batch_results):
    """Display batch processing results"""
    if not batch_results:
        st.warning("No batch results available")
        return
    
    st.markdown('<div class="section-header">📊 Batch Processing Results</div>', unsafe_allow_html=True)
    
    # Generate unique session ID for this batch to avoid key conflicts
    import time
    batch_session_id = int(time.time() * 1000) % 1000000  # Use timestamp for uniqueness
    
    # Summary metrics
    total_files = len(batch_results)
    successful = len([r for r in batch_results if r['status'] == 'success'])
    failed = len([r for r in batch_results if r['status'] in ['failed', 'error']])
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Files", total_files)
    with col2:
        st.metric("Successful", successful, delta=f"{(successful/total_files*100):.1f}%" if total_files > 0 else "0%")
    with col3:
        st.metric("Failed", failed, delta=f"{(failed/total_files*100):.1f}%" if total_files > 0 else "0%")
    with col4:
        total_chunks = sum([r.get('chunks_processed', 0) for r in batch_results if r['status'] == 'success'])
        st.metric("Total Chunks", total_chunks)
    
    # Add a clear results button
    if st.button("🗑️ Clear Results", key=f"clear_results_{batch_session_id}"):
        st.session_state.batch_results = None
        st.rerun()
    
    # Detailed results
    st.markdown("### 📝 Detailed Results")
    
    # Success/Failure tabs
    success_tab, failed_tab = st.tabs(["✅ Successful Conversions", "❌ Failed Conversions"])
    
    with success_tab:
        successful_results = [r for r in batch_results if r['status'] == 'success']
        if successful_results:
            for idx, result in enumerate(successful_results):
                with st.expander(f"✅ {os.path.basename(result['input_file'])}", expanded=False):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write(f"**Input:** `{result['input_file']}`")
                        st.write(f"**Output:** `{result['output_file']}`")
                        st.write(f"**Chunks Processed:** {result.get('chunks_processed', 'N/A')}")
                    
                    with col2:
                        python_valid = result.get('python_valid', False)
                        pyspark_valid = result.get('pyspark_valid', False)
                        
                        st.write(f"**Python Syntax:** {'✅' if python_valid else '❌'}")
                        st.write(f"**PySpark Syntax:** {'✅' if pyspark_valid else '❌'}")
                        
                        # Use session ID and index for guaranteed unique keys
                        if st.button(f"📁 Open Output Folder", key=f"open_success_{batch_session_id}_{idx}"):
                            st.info(f"Output saved to: {result['output_file']}")
        else:
            st.info("No successful conversions to display.")
    
    with failed_tab:
        failed_results = [r for r in batch_results if r['status'] in ['failed', 'error']]
        if failed_results:
            for idx, result in enumerate(failed_results):
                with st.expander(f"❌ {os.path.basename(result['input_file'])}", expanded=False):
                    st.write(f"**Input:** `{result['input_file']}`")
                    st.write(f"**Status:** {result['status']}")
                    st.error(f"**Error:** {result.get('error', 'Unknown error')}")
        else:
            st.success("No failed conversions! 🎉")

# Progress display function removed - using simpler approach

# Main UI
st.markdown('<h1 class="main-header">🔄 SAS Code Analyzer & Translator</h1>', unsafe_allow_html=True)

# Sidebar for file upload and controls
with st.sidebar:
    # Mode selection
    processing_mode = st.radio(
        "🔧 Processing Mode",
        ["Single File", "Batch Processing"],
        index=0
    )
    
    if processing_mode == "Single File":
        st.markdown("### 📁 Upload SAS Script")
        
        # File upload
        uploaded_file = st.file_uploader(
            "Choose a SAS file (.sas)",
            type=['sas', 'txt'],
            help="Upload your SAS script file"
        )
        
        # Text area for direct input
        st.markdown("### ✏️ Or Paste SAS Code")
        script_input = st.text_area(
            "Paste your SAS code here:",
            height=200,
            placeholder="/* Paste your SAS code here */\n%let input_ds = raw.sales;\ndata clean_sales;\n    set raw.sales;\n    /* Your code */\nrun;"
        )
        
        # Control buttons
        st.markdown("### 🎛️ Controls")
        
        col1, col2 = st.columns(2)
        
        with col1:
            analyze_btn = st.button(
                "🔍 Analyze",
                use_container_width=True,
                disabled=st.session_state.analysis_running
            )
        
        with col2:
            convert_btn = st.button(
                "🔄 Convert", 
                use_container_width=True,
                disabled=st.session_state.conversion_running
            )
        
        # Run both workflows
        run_both_btn = st.button("🚀 Run Both", use_container_width=True, type="primary")
    
    else:  # Batch Processing
        st.markdown("### 📂 Batch Processing")
        
        # Directory inputs
        input_directory = st.text_input(
            "Input Directory (SAS files):",
            placeholder="/path/to/sas/files",
            help="Directory containing .sas files to convert"
        )
        
        output_directory = st.text_input(
            "Output Directory (PySpark files):",
            placeholder="/path/to/output/pyspark",
            help="Directory where converted .py files will be saved"
        )
        
        # Processing options
        st.markdown("### ⚙️ Batch Options")
        
        col1, col2 = st.columns(2)
        
        with col1:
            max_workers = st.slider(
                "Parallel Workers:",
                min_value=1,
                max_value=6,
                value=3,
                help="Number of files to process in parallel (recommended: 2-4)"
            )
        
        with col2:
            rate_limit = st.slider(
                "Rate Limit (seconds):",
                min_value=0.1,
                max_value=2.0,
                value=0.5,
                step=0.1,
                help="Delay between LLM requests to prevent rate limiting"
            )
        
        enable_retries = st.checkbox(
            "Enable Auto-Retry",
            value=True,
            help="Automatically retry failed files up to 2 times"
        )
        
        # Batch processing button
        batch_btn = st.button(
            "🚀 Start Batch Processing",
            use_container_width=True,
            type="primary",
            disabled=st.session_state.batch_running or not input_directory or not output_directory
        )
        
        # Initialize variables for single file mode
        uploaded_file = None
        script_input = ""
        analyze_btn = False
        convert_btn = False
        run_both_btn = False

# Get script content
script_content = None
if uploaded_file is not None:
    script_content = str(uploaded_file.read(), "utf-8")
elif script_input.strip():
    script_content = script_input.strip()

# Display input script
if script_content:
    st.markdown('<div class="section-header">📝 Input SAS Script</div>', unsafe_allow_html=True)
    st.code(script_content, language="sql")

# Handle "Run Both" button
if run_both_btn and script_content:
    if not st.session_state.analysis_running and not st.session_state.conversion_running:
        st.session_state.analysis_running = True
        st.session_state.conversion_running = True
        
        with st.spinner("Running both workflows in parallel..."):
            try:
                analysis_result, conversion_result = run_both_workflows_parallel(script_content)
                
                if analysis_result:
                    st.session_state.analysis_state = analysis_result
                if conversion_result:
                    st.session_state.conversion_state = conversion_result
                
                st.success("✅ Both workflows completed!")
                
            except Exception as e:
                st.error(f"❌ Parallel execution error: {str(e)}")
            finally:
                st.session_state.analysis_running = False
                st.session_state.conversion_running = False
                st.rerun()

# Handle Batch Processing button
if processing_mode == "Batch Processing" and batch_btn:
    if not st.session_state.batch_running:
        # Clear previous results to avoid key conflicts
        st.session_state.batch_results = None
        
        # Validate directories
        if not os.path.exists(input_directory):
            st.error(f"❌ Input directory does not exist: {input_directory}")
        else:
            st.session_state.batch_running = True
            
            # Count SAS files
            sas_files = []
            for ext in ['*.sas', '*.SAS']:
                sas_files.extend(glob.glob(os.path.join(input_directory, ext)))
            
            if not sas_files:
                st.warning(f"⚠️ No SAS files found in {input_directory}")
                st.session_state.batch_running = False
            else:
                with st.spinner(f"Processing {len(sas_files)} SAS files with {max_workers} workers..."):
                    try:
                        # Run enhanced batch processing with user settings
                        results = run_batch_processing(
                            input_directory, 
                            output_directory, 
                            max_workers=max_workers,
                            rate_limit_delay=rate_limit,
                            enable_retries=enable_retries
                        )
                        st.session_state.batch_results = results
                        
                        # Summary
                        successful = len([r for r in results if r['status'] == 'success'])
                        failed = len([r for r in results if r['status'] in ['failed', 'error']])
                        
                        st.success(f"✅ Batch processing completed! {successful} successful, {failed} failed")
                        
                    except Exception as e:
                        st.error(f"❌ Batch processing error: {str(e)}")
                    finally:
                        st.session_state.batch_running = False
                        st.rerun()

# Main content area with tabs for full-width display
if processing_mode == "Single File" and script_content:
    # Create tabs for Analysis and Conversion
    analysis_tab, conversion_tab = st.tabs(["🔍 Analysis Workflow", "🔄 Conversion Workflow"])
    
    # Analysis Tab
    with analysis_tab:
        # Run analysis
        if analyze_btn and script_content and not st.session_state.analysis_running:
            st.session_state.analysis_running = True
            with st.spinner("Running analysis workflow..."):
                try:
                    result = run_analysis_workflow_simple(script_content)
                    if result:
                        st.session_state.analysis_state = result
                        st.success("✅ Analysis completed!")
                    else:
                        st.error("❌ Analysis failed!")
                except Exception as e:
                    st.error(f"❌ Analysis error: {str(e)}")
                finally:
                    st.session_state.analysis_running = False
                    st.rerun()
        
        # Display analysis results (full width)
        if st.session_state.analysis_state:
            display_analysis_results(st.session_state.analysis_state)
        elif st.session_state.analysis_running:
            st.info("🔄 Analysis in progress...")
        else:
            st.info("👆 Click 'Analyze' or 'Run Both' in the sidebar to start analysis")
    
    # Conversion Tab
    with conversion_tab:
        # Run conversion
        if convert_btn and script_content and not st.session_state.conversion_running:
            st.session_state.conversion_running = True
            with st.spinner("Running conversion workflow..."):
                try:
                    result = run_conversion_workflow_simple(script_content)
                    if result:
                        st.session_state.conversion_state = result
                        st.success("✅ Conversion completed!")
                    else:
                        st.error("❌ Conversion failed!")
                except Exception as e:
                    st.error(f"❌ Conversion error: {str(e)}")
                finally:
                    st.session_state.conversion_running = False
                    st.rerun()
        
        # Display conversion results (full width)
        if st.session_state.conversion_state:
            display_conversion_results(st.session_state.conversion_state)
        elif st.session_state.conversion_running:
            st.info("🔄 Conversion in progress...")
        else:
            st.info("👆 Click 'Convert' or 'Run Both' in the sidebar to start conversion")

elif processing_mode == "Batch Processing":
    # Show batch processing interface
    if st.session_state.batch_results:
        display_batch_results(st.session_state.batch_results)
    elif st.session_state.batch_running:
        st.info("🔄 Batch processing in progress...")
    else:
        st.info("👆 Configure directories and click 'Start Batch Processing' in the sidebar to begin.")
        
        # Show helpful information about batch processing
        with st.expander("ℹ️ About Batch Processing"):
            st.markdown("""
            **Batch Processing** allows you to convert multiple SAS files at once:
            
            1. **Input Directory**: Specify a folder containing your `.sas` files
            2. **Output Directory**: Choose where to save the converted `.py` files
            3. **Parallel Workers**: Control how many files are processed simultaneously
            
            **Features:**
            - ⚡ Parallel processing for faster conversion
            - 📊 Detailed progress and results tracking
            - 🔍 Syntax validation for all converted files
            - 📁 Organized output with clear file naming
            - ❌ Error reporting for failed conversions
            """)

elif processing_mode == "Single File":
    st.info("👆 Please upload a SAS file or paste SAS code in the sidebar to get started.")
    
    # Sample SAS code
    with st.expander("📋 Sample SAS Code (Click to expand)"):
        sample_code = """
/* Sample SAS ETL Pipeline */
%let input_ds = raw.sales_data;
%let threshold = 1000;
%let output_lib = work;

/* Step 1: Clean and prepare data */
data &output_lib..sales_clean;
    set &input_ds.;
    
    /* Remove invalid records */
    if region in ('', 'UNKNOWN') then delete;
    if amount <= 0 then delete;
    
    /* Create derived variables */
    revenue = price * quantity;
    profit_margin = (revenue - cost) / revenue;
    
    /* Categorize sales */
    if revenue > &threshold. then sales_category = 'HIGH';
    else if revenue > 500 then sales_category = 'MEDIUM';
    else sales_category = 'LOW';
    
    format revenue dollar12.2;
run;

/* Step 2: Aggregate by region */
proc sql;
    create table &output_lib..regional_summary as
    select 
        region,
        sales_category,
        count(*) as transaction_count,
        sum(revenue) as total_revenue,
        avg(profit_margin) as avg_margin
    from &output_lib..sales_clean
    group by region, sales_category
    having total_revenue > 100;
quit;

/* Step 3: Final report */
proc print data=&output_lib..regional_summary;
    title "Regional Sales Summary Report";
run;
"""
        st.code(sample_code, language="sql")

# Footer
st.markdown("---")

# Debug Section (expandable)
with st.expander("🐛 Debug Information"):
    try:
        st.write(f"**Analysis Pipeline Type:** {type(analysis_pipeline).__name__}")
        st.write(f"**Conversion Pipeline Type:** {type(conversion_pipeline).__name__}")
        st.write(f"**Analysis State:** {st.session_state.analysis_state is not None}")
        st.write(f"**Conversion State:** {st.session_state.conversion_state is not None}")
    except Exception as e:
        st.error(f"Debug error: {str(e)}")

st.markdown(
    """
    <div style='text-align: center; color: #666; padding: 1rem;'>
        Built with ❤️ using Streamlit • Powered by LangGraph & Azure OpenAI
    </div>
    """, 
    unsafe_allow_html=True
)