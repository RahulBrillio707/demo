from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, AIMessage
from langgraph.graph import MessagesState
from langchain_openai import ChatOpenAI, AzureChatOpenAI
from langgraph.graph import StateGraph, END, START
from langgraph.types import Command
from langgraph.prebuilt import create_react_agent
from langchain.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from langchain.schema.output_parser import StrOutputParser
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
    trim_messages,
)

import mermaid as md
import pyparsing as pp
import networkx as nx
import re 
from dotenv import load_dotenv
import os
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import ast

# Load environment
load_dotenv(override=True)

# Set up Azure OpenAI credentials
llm=AzureChatOpenAI(
   azure_deployment="gpt-4", 
   model_name="gpt-4o-mini", # or your deployment
   api_version="2025-01-01-preview",  # or your api version
   azure_endpoint="https://swapn-mfb5frew-eastus2.cognitiveservices.azure.com/",
   temperature=0,
   api_key="2TQatEAtwPIr2fIyVHgyqr7fwmgT3Hu5TClSIGvtTkPp3ydd3t71JQQJ99BIACHYHv6XJ3w3AAAAACOGiqNo"
)

# 1. Define the custom state for SAS analysis workflow
@dataclass
class SASAnalysisState:
    #  Inputs 
    file_path: Optional[str] = None
    script_content: Optional[str] = None
    chunk_size: int = 100
    
    #  Chunking outputs 
    chunks: List[str] = field(default_factory=list)
    element_summary: Dict[str, int] = field(default_factory=dict)
    num_chunks: int = 0
    script_length: Optional[int] = None
    
    #  AST extraction 
    ast_results: List[Dict[str, Any]] = field(default_factory=list)
    successful_asts: int = 0
    total_chunks: int = 0
    
    #  Script-level analysis 
    script_metadata: Dict[str, Any] = field(default_factory=dict)
    block_registry: List[Dict[str, Any]] = field(default_factory=list)
    block_connection_graph: List[Dict[str, Any]] = field(default_factory=list)
    data_flow_graph: List[List[str]] = field(default_factory=list)
    macro_map: Dict[str, Any] = field(default_factory=dict)
    variable_lineage_graph: List[Dict[str, Any]] = field(default_factory=list)
    semantic_summary: str = ""
    
    #  Block-level analysis 
    block_analysis_results: List[Dict[str, Any]] = field(default_factory=list)
    
    #  Visualization outputs 
    visualizations: Dict[str, Any] = field(default_factory=dict)
    
    #  Error capture 
    error_step: Optional[str] = None
    error_detail: Optional[Any] = None


# SAS Parsing Functions (copied from analyzer_react_test.py)
def remove_comments(sas_code: str) -> str:
    sas_code = re.sub(r"/\*.*?\*/", "", sas_code, flags=re.DOTALL)
    sas_code = re.sub(r"^\s*\*.*?;", "", sas_code, flags=re.MULTILINE)
    return sas_code

def define_sas_parser():
    macro_start = pp.CaselessKeyword("%MACRO") + pp.Word(pp.alphas + "_") + pp.restOfLine
    macro_end = pp.CaselessKeyword("%MEND") + pp.Optional(pp.Word(pp.alphas + "_")) + ";"
    proc_start = pp.CaselessKeyword("PROC") + pp.Word(pp.alphas) + pp.restOfLine
    proc_end = pp.CaselessKeyword("RUN") + ";"
    proc_sql_end = pp.CaselessKeyword("QUIT") + ";"
    data_start = pp.CaselessKeyword("DATA") + pp.Word(pp.alphas + "_") + pp.restOfLine

    macro_body = pp.originalTextFor(pp.SkipTo(macro_end, include=True))
    data_body = pp.originalTextFor(pp.SkipTo(proc_end | proc_sql_end | macro_end, include=True)) + proc_end
    proc_body = pp.originalTextFor(pp.SkipTo(proc_end | proc_sql_end | macro_end, include=True)) + (proc_end | proc_sql_end)

    macro = macro_start + macro_body + macro_end
    data_step = data_start + data_body
    proc_step = proc_start + proc_body
    return macro | data_step | proc_step

def parse_sas_code(sas_code: str) -> List[str]:
    sas_code = remove_comments(sas_code)
    parser = define_sas_parser()
    parsed_blocks = parser.searchString(sas_code)
    return [match[0] for match in parsed_blocks if match] or [sas_code]

def chunk_large_blocks(chunks: List[str], max_chunk_size: int) -> List[str]:
    sub_chunks = []
    for chunk in chunks:
        lines = chunk.split("\n")
        temp_chunk = []
        for line in lines:
            temp_chunk.append(line)
            if len(temp_chunk) >= max_chunk_size and line.strip().upper().endswith(("RUN;", "QUIT;", "%MEND;")):
                sub_chunks.append("\n".join(temp_chunk))
                temp_chunk = []
        if temp_chunk:
            sub_chunks.append("\n".join(temp_chunk))
    return sub_chunks

def build_dependency_graph(chunks: List[str]) -> nx.DiGraph:
    dag = nx.DiGraph()
    macro_references = {}
    for i, chunk in enumerate(chunks):
        dag.add_node(i, code=chunk)
        if "%MACRO" in chunk and "%MEND" in chunk:
            macro_name = re.search(r'%MACRO\s+(\w+)', chunk, re.IGNORECASE)
            if macro_name:
                macro_references[macro_name.group(1)] = i
    for i, chunk in enumerate(chunks):
        for macro_name, macro_index in macro_references.items():
            if f"%{macro_name}" in chunk and i != macro_index:
                dag.add_edge(macro_index, i)
    return dag

def split_overflow_chunks(chunk_list: List[dict], max_lines: int = 400) -> List[dict]:
    result = []
    logical_keywords = ("RUN;", "QUIT;", "%MEND;")

    for chunk in chunk_list:
        lines = chunk["code"].splitlines()
        if len(lines) <= max_lines:
            result.append(chunk)
            continue

        subchunks = []
        start = 0
        while start < len(lines):
            end = min(start + max_lines, len(lines))
            logical_end = -1

            for i in range(end - 1, start - 1, -1):
                if lines[i].strip().upper().endswith(logical_keywords):
                    logical_end = i + 1
                    break

            if logical_end == -1 or logical_end <= start:
                logical_end = end

            subchunk_lines = lines[start:logical_end]
            subchunks.append("\n".join(subchunk_lines))
            start = logical_end

        for j, sub in enumerate(subchunks):
            result.append({
                "id": f"{chunk['id']}_sub{j+1}",
                "code": sub.strip()
            })

    return result

def analyze_sas_elements(sas_code: str) -> Dict[str, int]:
    stats = {
        'comment': len(re.findall(r"/\*.*?\*/", sas_code, flags=re.DOTALL)) +
                   len(re.findall(r"^\s*\*.*?;", sas_code, flags=re.MULTILINE)),
        'macroVariableDefinition': len(re.findall(r"%let\s+\w+\s*=", sas_code, flags=re.IGNORECASE)),
        'libname': len(re.findall(r"\blibname\b", sas_code, flags=re.IGNORECASE)),
        'macro': len(re.findall(r"%macro\b", sas_code, flags=re.IGNORECASE)),
        'macroCall': len(re.findall(r"(?<!%)%\w+\b", sas_code)),
        'unparsedSQLStatement': len(re.findall(r"proc\s+sql\b", sas_code, flags=re.IGNORECASE)),
        'procedure': len(re.findall(r"\bproc\b", sas_code, flags=re.IGNORECASE)),
        'include': len(re.findall(r"%include", sas_code, flags=re.IGNORECASE)),
        'dataStep': len(re.findall(r"\bdata\b", sas_code, flags=re.IGNORECASE)),
    }
    return stats

# --- Workflow Nodes ---

def chunk_sas_script_node(state: SASAnalysisState) -> SASAnalysisState:
    print("[Node] chunk_sas_script_node")
    try:
        if state.file_path:
            # Handle file input
            with open(state.file_path, "r", encoding="utf-8") as f:
                script = f.read()
                state.script_content = script
        else:
            script = state.script_content or ""
        
        element_summary = analyze_sas_elements(script)
        initial_chunks = parse_sas_code(script)
        sub_chunks = chunk_large_blocks(initial_chunks, state.chunk_size)
        dag = build_dependency_graph(sub_chunks)
        
        try:
            ordered_chunks = [dag.nodes[i]["code"] for i in nx.topological_sort(dag)]
        except nx.NetworkXUnfeasible:
            print("� Cycle detected in macro calls! Falling back to original block order.")
            ordered_chunks = sub_chunks

        initial_result = [{"id": f"blk_{i+1:03}", "code": chunk.strip()} for i, chunk in enumerate(ordered_chunks)]
        final_chunks = split_overflow_chunks(initial_result, max_lines=400)

        state.chunks = [chunk["code"] for chunk in final_chunks]
        state.element_summary = element_summary
        state.num_chunks = len(final_chunks)
        state.script_length = len(script)
        
    except Exception as e:
        state.error_step = "chunk_sas_script"
        state.error_detail = str(e)
    
    return state

def extract_ast_node(state: SASAnalysisState) -> SASAnalysisState:
    print("[Node] extract_ast_node")
    print(f"Extracting AST from {len(state.chunks)} chunks...")
    
    try:
        ast_results = []
        
        # AST prompt template
        ast_prompt = ChatPromptTemplate.from_messages([
            ("system", 
             """You are an expert SAS code analyst. Analyze SAS code chunks and return a JSON object with the following fields:
             - type: One of [macro, dataStep, procStep, unknown]
             - name: The name of the macro, data step, or procedure (if applicable)
             - procType: If type is procStep, specify the PROC used (e.g., SQL, PRINT)
             - dependencies: A list of macro calls found inside the chunk (e.g., [clean, summary])
             - code: The raw SAS code (return exactly as received)
             - AST: A detailed abstract syntax tree representation
             Respond ONLY with a valid JSON object.
            """),
            ("human", """Here is an example input and output:

SAS Code Example:
/*============================================================================*/
/*      Example SAS ETL Script - Covers All Major Constructs                  */
/*============================================================================*/

%let input_ds = raw.transactions;
%let threshold = 1000;
%let outlib = work;

/* Define libname for input data */
libname raw '/data/incoming/';
libname archive '/data/archive/';

/* Include a utility macro from external file */
%include '/sas/macros/utils.sas';

/* Define a macro to process transactions */
%macro process_data(in=, out=, amount_threshold=);

    data &out.;
        set &in.;
        if amount > &amount_threshold then flag = 1;
        else flag = 0;

        /* Retain important variables across rows */
        retain user_id last_txn_date;

        /* Format date column */
        format txn_date date9.;

        /* Add a new derived column */
        txn_year = year(txn_date);

    run;

    proc sort data=&out.;
        by user_id txn_date;
    run;

    proc print data=&out.(obs=10);
    run;

%mend process_data;

/* Call the macro with specific parameters */
%process_data(in=&input_ds., out=&outlib..processed_txn, amount_threshold=&threshold.);

/* Merge with reference dataset */
data &outlib..final_txn;
    merge &outlib..processed_txn(in=a)
        archive.customer_info(in=b);
    by user_id;
    if a;
run;

/* Generate basic summary with PROC SQL */
proc sql;
    create table &outlib..summary as
    select user_id, count(*) as txn_count, sum(amount) as total_amount
    from &outlib..final_txn
    group by user_id
    having total_amount > &threshold;
quit;

/* Store results permanently if needed */
libname results '/data/output/';

data results.final_txn_summary;
    set &outlib..summary;
run;

Expected AST Output:
SASProgram(
    body=[
        CommentBlock(
            text='Example SAS ETL Script - Covers All Major Constructs'
        ),
        MacroVariableDefinition(name='input_ds', value='raw.transactions'),
        MacroVariableDefinition(name='threshold', value='1000'),
        MacroVariableDefinition(name='outlib', value='work'),

        LibnameAssignment(alias='raw', path='/data/incoming/'),
        LibnameAssignment(alias='archive', path='/data/archive/'),

        IncludeMacro(file='/sas/macros/utils.sas'),

        MacroDefinition(
            name='process_data',
            parameters=['in', 'out', 'amount_threshold'],
            body=[
                DataStep(
                    name='&out.',
                    source='&in.',
                    operations=[
                        ConditionalAssignment(
                            condition='amount > &amount_threshold',
                            then='flag = 1',
                            else_='flag = 0'
                        ),
                        RetainStatement(variables=['user_id', 'last_txn_date']),
                        FormatStatement(variable='txn_date', format='date9.'),
                        DerivedColumnAssignment(
                            name='txn_year',
                            expression='year(txn_date)'
                        )
                    ]
                ),
                ProcSort(
                    data='&out.',
                    by=['user_id', 'txn_date']
                ),
                ProcPrint(
                    data='&out.',
                    options={{'obs': 10}}
                )
            ]
        ),

        MacroCall(
            name='process_data',
            arguments={{
                'in': '&input_ds.',
                'out': '&outlib..processed_txn',
                'amount_threshold': '&threshold.'
            }}
        ),

        DataStep(
            name='&outlib..final_txn',
            source='MERGE',
            merge_sources=[
                {{'dataset': '&outlib..processed_txn', 'alias': 'a'}},
                {{'dataset': 'archive.customer_info', 'alias': 'b'}}
            ],
            by='user_id',
            filter='if a'
        ),

        ProcSQL(
            operation='create table',
            table='&outlib..summary',
            select=[
                'user_id',
                'count(*) as txn_count',
                'sum(amount) as total_amount'
            ],
            from_='&outlib..final_txn',
            group_by='user_id',
            having='total_amount > &threshold'
        ),

        LibnameAssignment(alias='results', path='/data/output/'),

        DataStep(
            name='results.final_txn_summary',
            source='&outlib..summary'
        )
    ]
)

Now analyze the following SAS code chunk:
{chunk_code}

Expected JSON output:
""")
        ])
        ast_chain = ast_prompt | llm | JsonOutputParser()
        
        for i, chunk in enumerate(state.chunks):
            try:
                print(f"Processing chunk {i+1}/{len(state.chunks)}")
                ast_result = ast_chain.invoke({"chunk_code": chunk})
                
                ast_results.append({
                    "chunk_index": i,
                    "response": ast_result,
                    "chunk": chunk
                })
                
            except Exception as e:
                print(f"Error processing chunk {i+1}: {e}")
                ast_results.append({
                    "chunk_index": i,
                    "error": str(e),
                    "chunk": chunk
                })
        
        state.ast_results = ast_results
        state.total_chunks = len(state.chunks)
        state.successful_asts = len([ast for ast in ast_results if "error" not in ast])
        
    except Exception as e:
        state.error_step = "extract_ast"
        state.error_detail = str(e)
        
    return state

def script_level_analysis_node(state: SASAnalysisState) -> SASAnalysisState:
    print("[Node] script_level_analysis_node")
    
    try:
        script_level_prompt = ChatPromptTemplate.from_messages([
            ("system", "You are an expert SAS code analyzer that performs comprehensive static analysis on SAS scripts to extract their structure, data flow, and variable relationships."),
            ("human", """
INSTRUCTIONS:
1. Analyze the complete structure of the SAS script
2. Identify all code blocks (DATA steps, PROC SQL, macro definitions, macro calls)
3. Track all datasets and their transformations
4. Map the flow of variables through the script
5. Document macro definitions and their usage
6. Create a concise semantic summary of the script's purpose

OUTPUT STRUCTURE:
Generate a JSON with the following components:
- script_metadata: Basic information about the script.
- block_registry: Detailed inventory of all code blocks.
- block_connection_graph: Connections between blocks.
- data_flow_graph: Dataset-level flow representation.
- macro_map: All macros with their parameters and usage.
- variable_lineage_graph: Complete variable transformation history.
- semantic_summary: High-level description of script purpose.
EXAMPLE:
            Given this SAS script:

            ```sas
            /* Step 1: Import and clean */
            data sales_clean;
                set raw.sales_data;
                if region ^= 'WEST' then delete;
                revenue = price * quantity;
                if revenue < 0 then revenue = 0;
            run;

            /* Step 2: Aggregate */
            proc sql;
                create table region_summary as
                select region, sum(revenue) as total_rev
                from sales_clean
                group by region;
            quit;

            /* Step 3: Join with targets */
            data final_output;
                merge region_summary(in=a) targets(in=b);
                by region;
                if a and b;
            run;

            /* Step 4: Apply macro logic */
            %macro threshold_flag(tbl, col, thresh);
                data &tbl._flagged;
                    set &tbl;
                    if &col > &thresh then flag = 1;
                    else flag = 0;
                run;
            %mend;

            %threshold_flag(final_output, total_rev, 100000);
            ```
            The expected output is:
            {{
            "script_metadata": {{
                "script_name": "sales_pipeline.sas",
                "num_blocks": 5,
                "macros_defined": 1,
                "macros_invoked": 1
            }},

            "block_registry": [
                {{
                "block_id": 0,
                "type": "SOURCE",
                "name": "raw.sales_data",
                "outputs": ["price", "quantity", "region"]
                }},
                {{
                "block_id": 1,
                "type": "DATA_STEP",
                "name": "Clean and enrich sales data",
                "inputs": ["raw.sales_data"],
                "outputs": ["sales_clean"]
                }},
                {{
                "block_id": 2,
                "type": "PROC_SQL",
                "name": "Aggregate revenue by region",
                "inputs": ["sales_clean"],
                "outputs": ["region_summary"]
                }},
                {{
                "block_id": 3,
                "type": "DATA_STEP",
                "name": "Join with targets",
                "inputs": ["region_summary", "targets"],
                "outputs": ["final_output"]
                }},
                {{
                "block_id": 4,
                "type": "MACRO_DEF",
                "name": "threshold_flag",
                "params": ["tbl", "col", "thresh"]
                }},
                {{
                "block_id": 5,
                "type": "MACRO_CALL",
                "name": "apply_threshold_flag",
                "inputs": ["final_output"],
                "outputs": ["final_output_flagged"],
                "macro": "threshold_flag"
                }}
            ],

            "block_connection_graph": [
                {{
                "from_block": 0,
                "to_block": 1,
                "via": "raw.sales_data"
                }},
                {{
                "from_block": 1,
                "to_block": 2,
                "via": "sales_clean"
                }},
                {{
                "from_block": 2,
                "to_block": 3,
                "via": "region_summary"
                }},
                {{
                "from_block": "external",
                "to_block": 3,
                "via": "targets"
                }},
                {{
                "from_block": 3,
                "to_block": 5,
                "via": "final_output"
                }},
                {{
                "from_block": 4,
                "to_block": 5,
                "via_macro": "threshold_flag"
                }}
            ],

            "data_flow_graph": [
                ["raw.sales_data", "sales_clean"],
                ["sales_clean", "region_summary"],
                ["region_summary", "final_output"],
                ["targets", "final_output"],
                ["final_output", "final_output_flagged"]
            ],

            "macro_map": {{
                "threshold_flag": {{
                "params": ["tbl", "col", "thresh"],
                "definition_block_id": 4,
                "invocations": [5]
                }}
            }},

            "variable_lineage_graph": [
                {{
                "source_dataset": "raw.sales_data",
                "source_variables": ["price", "quantity", "region"],
                "defined_in_block": 0,
                "operation": "source columns"
                }},
                {{
                "target_variable": "revenue",
                "sources": ["price", "quantity"],
                "operation": "price * quantity",
                "defined_in_block": 1,
                "output_dataset": "sales_clean"
                }},
                {{
                "target_variable": "revenue",
                "sources": ["revenue"],
                "operation": "if revenue < 0 then revenue = 0",
                "defined_in_block": 1,
                "output_dataset": "sales_clean"
                }},
                {{
                "target_variable": "total_rev",
                "sources": ["revenue", "region"],
                "operation": "sum(revenue) grouped by region",
                "defined_in_block": 2,
                "output_dataset": "region_summary"
                }},
                {{
                "target_variable": "region_summary",
                "sources": ["region", "total_rev"],
                "operation": "output table from aggregation",
                "defined_in_block": 2
                }},
                {{
                "target_dataset": "final_output",
                "sources": ["region_summary", "targets"],
                "operation": "merge on region",
                "defined_in_block": 3
                }},
                {{
                "target_variable": "flag",
                "sources": ["total_rev"],
                "operation": "if total_rev > 100000 then flag = 1 else 0",
                "defined_in_block": 5,
                "output_dataset": "final_output_flagged"
                }},
                {{
                "target_dataset": "final_output_flagged",
                "sources": ["final_output", "flag"],
                "operation": "macro: threshold_flag",
                "defined_in_block": 5
                }}
            ],

            "semantic_summary": "Clean sales data → summarize revenue → join with targets → flag high performers"
        }}
Now analyze the following SAS script:
```sas
{script}
```
Please respond only with the JSON output.
""")
        ])
        
        script_level_chain = script_level_prompt | llm | JsonOutputParser()
        combined_script = "\n\n".join(state.chunks)
        result = script_level_chain.invoke({"script": combined_script})
        
        # Update state with results
        state.script_metadata = result.get("script_metadata", {})
        state.block_registry = result.get("block_registry", [])
        state.block_connection_graph = result.get("block_connection_graph", [])
        state.data_flow_graph = result.get("data_flow_graph", [])
        state.macro_map = result.get("macro_map", {})
        state.variable_lineage_graph = result.get("variable_lineage_graph", [])
        state.semantic_summary = result.get("semantic_summary", "")
        
    except Exception as e:
        state.error_step = "script_level_analysis"
        state.error_detail = str(e)
        
    return state

def block_level_analysis_node(state: SASAnalysisState) -> SASAnalysisState:
    print("[Node] block_level_analysis_node")
    
    try:
        block_level_prompt = ChatPromptTemplate.from_messages([
            ("system", "You are an expert SAS code analyzer that performs detailed block-level analysis of SAS scripts."),
            ("human", """
INSTRUCTIONS:
1. Identify all distinct code blocks in the SAS script
2. For each block, determine:
   - Block type (DATA_STEP, PROC_SQL, MACRO_DEF, MACRO_CALL, etc.)
   - Input and output datasets
   - Variables used and created
   - Abstract syntax tree (AST) representation
   - Semantic intent and operations
   - Variable lineage relationships

OUTPUT STRUCTURE:
            Generate a JSON array where each element represents a block with the following properties:
            - block_id: Unique identifier for the block
            - type: Type of the block (DATA_STEP, PROC_SQL, MACRO_DEF, MACRO_CALL)
            - input_datasets: Datasets read by this block
            - output_datasets: Datasets created or modified by this block
            - used_variables: Variables referenced from input datasets
            - created_variables: New variables created in this block
            - ast: Abstract syntax tree representing the code structure
            - semantic_model: Higher-level description of the block's purpose and operations
            - lineage_links: Mapping of how variables are derived from other variables

        EXAMPLE:
            Given this SAS script:

            ```sas
            /* Step 1: Import and clean */
            data sales_clean;
                set raw.sales_data;
                if region ^= 'WEST' then delete;
                revenue = price * quantity;
                if revenue < 0 then revenue = 0;
            run;

            /* Step 2: Aggregate */
            proc sql;
                create table region_summary as
                select region, sum(revenue) as total_rev
                from sales_clean
                group by region;
            quit;

            /* Step 3: Join with targets */
            data final_output;
                merge region_summary(in=a) targets(in=b);
                by region;
                if a and b;
            run;

            /* Step 4: Apply macro logic */
            %macro threshold_flag(tbl, col, thresh);
                data &tbl._flagged;
                    set &tbl;
                    if &col > &thresh then flag = 1;
                    else flag = 0;
                run;
            %mend;

            %threshold_flag(final_output, total_rev, 100000);
            ```
        The expected output is:

        [
        {{
            "block_id": 1,
            "type": "DATA_STEP",
            "input_datasets": ["raw.sales_data"],
            "output_datasets": ["sales_clean"],
            "used_variables": ["region", "price", "quantity"],
            "created_variables": ["revenue"],
            "ast": {{
            "type": "DATA_STEP",
            "statements": [
                {{"type": "CONDITION", "condition": "region ^= 'WEST'", "action": "delete"}},
                {{"type": "COMPUTATION", "lhs": "revenue", "rhs": "price * quantity"}},
                {{"type": "IF", "condition": "revenue < 0", "then": "revenue = 0"}}
            ]
            }},
            "semantic_model": {{
            "intent": "Clean and enrich sales data",
            "operations": ["filter", "compute", "conditional override"],
            "data_flow": {{
                "revenue": "price * quantity"
            }}
            }},
            "lineage_links": {{
            "revenue": ["price", "quantity"]
            }}
        }},
        {{
            "block_id": 2,
            "type": "PROC_SQL",
            "input_datasets": ["sales_clean"],
            "output_datasets": ["region_summary"],
            "used_variables": ["region", "revenue"],
            "created_variables": ["total_rev"],
            "ast": {{
            "type": "PROC_SQL",
            "statements": [
                {{
                "type": "CREATE_TABLE",
                "table": "region_summary",
                "query": {{
                    "type": "SELECT",
                    "columns": ["region", {{"function": "sum", "arg": "revenue", "alias": "total_rev"}}],
                    "from": "sales_clean",
                    "groupby": ["region"]
                }}
                }}
            ]
            }},
            "semantic_model": {{
            "intent": "Aggregate revenue by region",
            "operations": ["aggregation", "grouping"],
            "data_flow": {{
                "total_rev": "sum(revenue) grouped by region"
            }}
            }},
            "lineage_links": {{
            "total_rev": ["revenue"]
            }}
        }},
        {{
            "block_id": 3,
            "type": "DATA_STEP",
            "input_datasets": ["region_summary", "targets"],
            "output_datasets": ["final_output"],
            "used_variables": ["region"],
            "created_variables": [],
            "ast": {{
            "type": "DATA_STEP",
            "statements": [
                {{"type": "MERGE", "datasets": ["region_summary(in=a)", "targets(in=b)"], "by": "region"}},
                {{"type": "CONDITION", "condition": "a and b", "action": "keep"}}
            ]
            }},
            "semantic_model": {{
            "intent": "Join with targets",
            "operations": ["merge", "filter"],
            "data_flow": {{
                "final_output": "merge of region_summary and targets on region"
            }}
            }},
            "lineage_links": {{
            "final_output": ["region_summary", "targets"]
            }}
        }},
        {{
            "block_id": 4,
            "type": "MACRO_DEF",
            "input_datasets": [],
            "output_datasets": [],
            "used_variables": [],
            "created_variables": [],
            "ast": {{
            "type": "MACRO_DEF",
            "name": "threshold_flag",
            "parameters": ["tbl", "col", "thresh"],
            "body": {{
                "type": "DATA_STEP",
                "statements": [
                {{"type": "SET", "dataset": "&tbl"}},
                {{"type": "IF", "condition": "&col > &thresh", "then": "flag = 1", "else": "flag = 0"}}
                ]
            }}
            }},
            "semantic_model": {{
            "intent": "Define threshold flag macro",
            "operations": ["macro definition", "conditional flag assignment"],
            "data_flow": {{
                "flag": "based on threshold comparison"
            }}
            }},
            "lineage_links": {{}}
        }},
        {{
            "block_id": 5,
            "type": "MACRO_CALL",
            "input_datasets": ["final_output"],
            "output_datasets": ["final_output_flagged"],
            "used_variables": ["total_rev"],
            "created_variables": ["flag"],
            "ast": {{
            "type": "MACRO_CALL",
            "name": "threshold_flag",
            "arguments": ["final_output", "total_rev", "100000"]
            }},
            "semantic_model": {{
            "intent": "Apply threshold flagging to data",
            "operations": ["conditional flagging"],
            "data_flow": {{
                "flag": "total_rev > 100000 ? 1 : 0"
            }}
            }},
            "lineage_links": {{
            "flag": ["total_rev"]
            }}
        }}
        ]
Now analyze the following SAS script and generate block-level analysis:
```sas
{script}
```
Please respond only with the JSON array containing the block-level analysis.
""")
        ])
        
        block_level_chain = block_level_prompt | llm | JsonOutputParser()
        combined_script = "\n\n".join(state.chunks)
        result = block_level_chain.invoke({"script": combined_script})
        
        state.block_analysis_results = result if isinstance(result, list) else []
        
    except Exception as e:
        state.error_step = "block_level_analysis"
        state.error_detail = str(e)
        
    return state

def generate_visualizations_node(state: SASAnalysisState) -> SASAnalysisState:
      print("[Node] generate_visualizations_node")

      # Create analysis_json dict from state for compatibility with reference function
      analysis_json = {
          "block_registry": state.block_registry,
          "block_connection_graph": state.block_connection_graph,
          "data_flow_graph": state.data_flow_graph,
          "variable_lineage_graph": state.variable_lineage_graph
      }

      # Handle the input validation (from reference)
      if not isinstance(analysis_json, dict):
          print(f"Error: Expected a dictionary, got {type(analysis_json)}")
          state.error_step = "generate_visualizations"
          state.error_detail = f"Expected a dictionary, got {type(analysis_json)}"
          return state

      if "block_registry" not in analysis_json:
          print("Error: 'block_registry' not found in input")
          if isinstance(analysis_json, dict) and len(analysis_json) > 0:
              print(f"Available keys: {list(analysis_json.keys())}")
          state.error_step = "generate_visualizations"
          state.error_detail = "'block_registry' not found in input"
          return state

      # Print debug info
      print(f"Input type: {type(analysis_json)}")
      print(f"Input keys: {list(analysis_json.keys() if isinstance(analysis_json, dict) else [])}")

      # Create three chains directly
      # 1. Block Flow Diagram Chain
      block_flow_prompt = ChatPromptTemplate.from_messages([
          ("system", """You are an expert in creating Mermaid.js diagrams. Your task is to generate a block flow diagram showing 
  how code blocks are connected."""),
          ("human", """
              Create a Mermaid diagram (graph TD) that visualizes the connections between code blocks in a SAS script.
              
              BLOCK REGISTRY:
              ```json
              {block_registry}
              ```
              
              CONNECTIONS:
              ```json
              {block_connections}
              ```
              
              REQUIREMENTS:
              1. Use graph TD syntax for top-down flow
              2. Create a node for each block in the registry
              3. Format block labels with TWO LINES: First line should be "blockid: block_type", second line should be the 
  block name
              4. Connect the nodes according to the connections
              5. Style SOURCE blocks with fill:#f9f,stroke:#333
              6. Style MACRO blocks with fill:#bbf,stroke:#333
              7. Style OUTPUT blocks with fill:#bfb,stroke:#333
              8. Handle "external" connections appropriately
              9. Label edges with the "via" value
              10. DO NOT use parentheses ( ) in the Mermaid code as they cause errors - use spaces only
              11. Keep the diagram clean and readable
              
              Return ONLY the Mermaid diagram code without any explanation or markdown formatting.
          """)
      ])
      block_flow_chain = block_flow_prompt | llm | StrOutputParser()

      # 2. Data Transformation Diagram Chain
      data_flow_prompt = ChatPromptTemplate.from_messages([
          ("system", """You are an expert in creating Mermaid.js diagrams. Your task is to generate a data flow diagram showing 
  dataset transformations."""),
          ("human", """
              Create a Mermaid diagram (graph LR) that visualizes the data transformations between datasets in a SAS script.
              
              DATA FLOW:
              ```json
              {data_flow}
              ```
              
              REQUIREMENTS:
              1. Use graph LR syntax for left-to-right flow
              2. Create a node for each unique dataset
              3. Connect datasets according to the data flow (each entry is [source, target])
              4. Style source datasets (containing "raw" or ".") with fill:#f9f,stroke:#333
              5. Style final datasets (ending with "_flagged" or "_final") with fill:#bfb,stroke:#333
              6. Label edges with "transform"
              7. Replace "." with "_" in node IDs but preserve the original text in labels
              8. DO NOT use parentheses ( ) in the Mermaid code as they cause errors - use spaces only
              9. Keep the diagram clean and readable
              
              Return ONLY the Mermaid diagram code without any explanation or markdown formatting.
          """)
      ])
      data_flow_chain = data_flow_prompt | llm | StrOutputParser()

      # 3. Variable Lineage Diagram Chain
      var_lineage_prompt = ChatPromptTemplate.from_messages([
          ("system", """You are an expert in creating Mermaid.js diagrams. Your task is to generate a variable lineage diagram 
  showing how variables are derived from other variables. CRITICAL: DO NOT use ANY parentheses in the diagram as they cause 
  syntax errors in Mermaid."""),
          ("human", """
              Create a Mermaid diagram (graph TD) that visualizes variable lineage and transformations in a SAS script.
              
              VARIABLE LINEAGE:
              ```json
              {var_lineage}
              ```
              
              REQUIREMENTS:
              1. Use graph TD syntax for top-down flow
              2. Identify source variables (found in entries with "source_variables")
              3. Identify derived variables (found in entries with "target_variable")
              4. Create a subgraph for source variables with fill:#f9f,stroke:#333
              5. Create a subgraph for derived variables with fill:#bfb,stroke:#333
              6. Connect variables according to their relationships
              7. Label edges with the "operation" value from the lineage entries
              8. ⚠️ CRITICAL: DO NOT use ANY parentheses ( ) in the Mermaid code - they cause syntax errors
              9. Replace ALL parentheses with square brackets [ ] or remove them entirely
              10. For node labels, use quotes and spaces instead of parentheses: "variable name" NOT "variable(param)"
              11. Keep the diagram clean and readable
              
              Return ONLY the Mermaid diagram code without any explanation or markdown formatting.
          """)
      ])
      var_lineage_chain = var_lineage_prompt | llm | StrOutputParser()

      # Generate all diagrams by calling chains one by one
      try:
          diagrams = {}

          # Call chain 1: Block Flow Diagram
          block_flow_code = block_flow_chain.invoke({
              "block_registry": analysis_json.get("block_registry", []),
              "block_connections": analysis_json.get("block_connection_graph", [])
          })
          diagrams["block_flow_diagram"] = {
              "mermaid_code": block_flow_code,
              "description": "Shows how code blocks are connected and data flows between them",
              "suggested_size": {"width": 800, "height": 600},
              "category": "flow",
              "priority": 1
          }

          # Call chain 2: Data Transformation Diagram
          data_flow_code = data_flow_chain.invoke({
              "data_flow": analysis_json.get("data_flow_graph", [])
          })
          diagrams["data_transformation_diagram"] = {
              "mermaid_code": data_flow_code,
              "description": "Shows how datasets are transformed through the script",
              "suggested_size": {"width": 700, "height": 400},
              "category": "data",
              "priority": 2
          }

          # Call chain 3: Variable Lineage Diagram
          var_lineage_code = var_lineage_chain.invoke({
              "var_lineage": analysis_json.get("variable_lineage_graph", [])
          })
          diagrams["variable_lineage_diagram"] = {
              "mermaid_code": var_lineage_code,
              "description": "Shows variable derivations and transformations",
              "suggested_size": {"width": 600, "height": 500},
              "category": "variable",
              "priority": 3
          }

          state.visualizations = diagrams

      except Exception as e:
          print(f"Error generating visualizations: {str(e)}")
          state.error_step = "generate_visualizations"
          state.error_detail = str(e)

      return state
# --- Graph Construction ---

graph = StateGraph(SASAnalysisState)

# Add nodes
graph.add_node("chunk_node", chunk_sas_script_node)
graph.add_node("ast_node", extract_ast_node)
graph.add_node("script_analysis_node", script_level_analysis_node)
graph.add_node("block_analysis_node", block_level_analysis_node)
graph.add_node("visualization_node", generate_visualizations_node)

# Define edges
graph.add_edge(START, "chunk_node")
graph.add_edge("chunk_node", "ast_node")
graph.add_edge("ast_node", "script_analysis_node")
graph.add_edge("script_analysis_node", "block_analysis_node")
graph.add_edge("block_analysis_node", "visualization_node")
graph.add_edge("visualization_node", END)

# Compile the graph
analysis_pipeline = graph.compile()

# Test the workflow
if __name__ == "__main__":
    test_script = """
    %let input_ds = raw.sales;
    %let threshold = 1000;
    
    data clean_sales;
        set raw.sales;
        if amount > &threshold then flag = 'HIGH';
        else flag = 'LOW';
    run;
    
    proc sql;
        create table summary as
        select flag, count(*) as count
        from clean_sales
        group by flag;
    quit;
    """
    
    init_state = SASAnalysisState(
        script_content=test_script,
        chunk_size=100
    )
    
    final_state = analysis_pipeline.invoke(init_state)
    
    print("=== Analysis Results ===")
    print(f"Chunks: {len(final_state.chunks)}")
    print(f"AST Results: {len(final_state.ast_results)}")
    print(f"Block Registry: {len(final_state.block_registry)}")
    print(f"Visualizations: {len(final_state.visualizations)}")
    print(f"Semantic Summary: {final_state.semantic_summary}")
    
    if final_state.error_step:
        print(f"Error at step: {final_state.error_step}")
        print(f"Error detail: {final_state.error_detail}")