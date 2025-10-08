from langchain_core.tools import tool
from langchain_core.messages import HumanMessage, AIMessage
from langgraph.graph import MessagesState
# from langchain_aws import ChatBedrock,
from langchain_openai import ChatOpenAI,AzureChatOpenAI
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


# Set up Azure OpenAI credentials
os.environ["AZURE_OPENAI_API_KEY"] = 'EOpDX9B2g0n6HYPMMTSJCeN00D6leGgpflKvp5UROz4y1vmu5MTmJQQJ99BAACHYHv6XJ3w3AAAAACOGl6ms'
os.environ["AZURE_OPENAI_ENDPOINT"] = 'https://brillioagentsh8218774988.openai.azure.com'

llm = AzureChatOpenAI(
    azure_deployment="gpt-4o-2",
    api_version="2024-12-01-preview",
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
)


# # Import your existing tool implementations:
# from react_test_v2 import parse_sas_file, parse_sas_code, analyze_sas_elements
# from react_test_v2 import (
#     chunk_sas_file,
#     chunk_sas_script,
#     extract_ast_with_llm,
#     translate_sas_chunk,
#     merge_and_deduplicate_code,
#     validate_python_syntax,
#     validate_pyspark_syntax,
# )


# 1. Define the full custom state
@dataclass
class SASPipelineState:
    # — Inputs —
    file_path:       Optional[str]     = None
    script_content:  Optional[str]     = None
    chunk_size:      int               = 100   # default chunk size

    # — Chunking outputs —
    chunks:          List[str]         = field(default_factory=list)
    element_summary: Dict[str,int]     = field(default_factory=dict)
    num_chunks:      int               = 0
    script_length:   Optional[int]     = None

    # — AST extraction —
    chunks_with_ast: List[Dict[str,str]] = field(default_factory=list)

    # — Translation outputs —
    translated_chunks: List[Dict[str,Any]] = field(default_factory=list)

    # — Merge & dedupe —
    merged_sas_code:       str = ""
    merged_python_code:    str = ""
    merged_pyspark_code:   str = ""
    python_blocks_count:   int = 0
    pyspark_blocks_count:  int = 0
    sas_blocks_count:      int = 0
    total_chunks_processed:int = 0

    # — Validation results —
    python_validation:   Dict[str,Any] = field(default_factory=dict)
    pyspark_validation:  Dict[str,Any] = field(default_factory=dict)

    # — Error capture —
    error_step:         Optional[str]   = None
    error_detail:       Optional[Any]   = None


# --- Tool logic implementations as direct functions ---
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

def parse_sas_code(sas_code: str) -> list:
    sas_code = remove_comments(sas_code)
    parser = define_sas_parser()
    parsed_blocks = parser.searchString(sas_code)
    return [match[0] for match in parsed_blocks if match] or [sas_code]

def chunk_large_blocks(chunks: list, max_chunk_size: int) -> list:
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

def build_dependency_graph(chunks: list) -> nx.DiGraph:
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

def split_overflow_chunks(chunk_list: list, max_lines: int = 400) -> list:
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

def analyze_sas_elements(sas_code: str) -> dict:
    return {
        'comment': len(re.findall(r"/\*.*?\*/", sas_code, flags=re.DOTALL)) +
                   len(re.findall(r"^\s*\*.*?;", sas_code, flags=re.MULTILINE)),
        'macroVariableDefinition': len(re.findall(r"%let\s+\w+\s*=", sas_code, flags=re.IGNORECASE)),
        'libname': len(re.findall(r"\\blibname\\b", sas_code, flags=re.IGNORECASE)),
        'macro': len(re.findall(r"%macro\\b", sas_code, flags=re.IGNORECASE)),
        'macroCall': len(re.findall(r"(?<!%)%\\w+\\b", sas_code)),
        'unparsedSQLStatement': len(re.findall(r"proc\s+sql\\b", sas_code, flags=re.IGNORECASE)),
        'procedure': len(re.findall(r"\\bproc\\b", sas_code, flags=re.IGNORECASE)),
        'include': len(re.findall(r"%include", sas_code, flags=re.IGNORECASE)),
        'dataStep': len(re.findall(r"\\bdata\\b", sas_code, flags=re.IGNORECASE)),
    }


# --- Nodes ---


def chunk_sas_script_node(state: SASPipelineState) -> SASPipelineState:
    print("[Node] chunk_sas_script")
    try:
        script = state.script_content or ""
        element_summary = analyze_sas_elements(script)
        initial_chunks = parse_sas_code(script)
        sub_chunks = chunk_large_blocks(initial_chunks, state.chunk_size)
        dag = build_dependency_graph(sub_chunks)
        try:
            ordered_chunks = [dag.nodes[i]["code"] for i in nx.topological_sort(dag)]
        except nx.NetworkXUnfeasible:
            print("⚠️ Cycle detected in macro calls! Falling back to original block order.")
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

def extract_ast_node(state: SASPipelineState) -> SASPipelineState:
    print("[Node] extract_ast_node")
    print(f"Extracting AST from {len(state.chunks)} chunks...")

    chunks_with_ast = []

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
    ("human", """
    Here is an example input and output:

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
            chunk_ast = ast_result.get("AST", "No AST generated")

            chunks_with_ast.append({
                "chunk_id": f"chunk_{i+1:03}",
                "chunk_code": chunk,
                "chunk_ast": chunk_ast
            })

        except Exception as e:
            print(f"Error processing chunk {i+1}: {e}")
            chunks_with_ast.append({
                "chunk_id": f"chunk_{i+1:03}",
                "chunk_code": chunk,
                "chunk_ast": f"Error generating AST: {str(e)}"
            })

    state.chunks_with_ast = chunks_with_ast
    return state

def translate_chunk_node(state: SASPipelineState) -> SASPipelineState:
    print("[Node] translate_chunk_node")
    print(f"Translating {len(state.chunks_with_ast)} SAS chunks to Python and PySpark...")

    translated_chunks = []

    translation_prompt = ChatPromptTemplate.from_messages([
        ("system",""""
            You are an expert SAS-to-Python/PySpark translator. Translate the following SAS code chunk to both Python and PySpark.

            Return a JSON object with the following structure:
            {{
                "python_code": "equivalent Python code",
                "pyspark_code": "equivalent PySpark code",
                "translation_notes": "any important notes about the translation"
            }}

            Focus on maintaining the same logic and functionality while adapting to Python/PySpark syntax and paradigms.
            """),
        ("human", """      
                SAS Code:
                {chunk_code}

                AST Information: {chunk_ast}

                Provide translations for both Python and PySpark. Consider:
                - Data manipulation operations
                - Statistical procedures
                - Macro logic
                - File I/O operations
                - Database operations
            """)
    ])
    translation_chain = translation_prompt | llm | JsonOutputParser()

    for i, chunk_data in enumerate(state.chunks_with_ast):
        print(f"Translating chunk {i+1}/{len(state.chunks_with_ast)}")

        try:
            chunk_id = chunk_data.get("chunk_id", f"chunk_{i+1}")
            chunk_code = chunk_data.get("chunk_code", "")
            chunk_ast = chunk_data.get("chunk_ast", "")

            result = translation_chain.invoke({
                "chunk_code": chunk_code,
                "chunk_ast": chunk_ast
            })

            translated_chunks.append({
                "chunk_id": chunk_id,
                "chunk_sas_code": chunk_code,
                "chunk_ast": chunk_ast,
                "chunk_python_code": result.get("python_code", ""),
                "chunk_pyspark_code": result.get("pyspark_code", ""),
                "translation_notes": result.get("translation_notes", "")
            })

        except Exception as e:
            print(f"Error translating chunk {i+1}: {str(e)}")
            translated_chunks.append({
                "chunk_id": chunk_data.get("chunk_id", f"chunk_{i+1}"),
                "chunk_sas_code": chunk_data.get("chunk_code", ""),
                "chunk_ast": chunk_data.get("chunk_ast", ""),
                "chunk_python_code": f"# Error: {str(e)}",
                "chunk_pyspark_code": f"# Error: {str(e)}",
                "translation_notes": f"Translation failed: {str(e)}"
            })

    state.translated_chunks = translated_chunks
    return state

def merge_code_node(state: SASPipelineState) -> SASPipelineState:
    print("[Node] merge_code_node")
    print("Merging and deduplicating code blocks...")

    try:
        python_blocks = []
        pyspark_blocks = []
        sas_blocks = []

        for chunk in state.translated_chunks:
            python_code = chunk.get("chunk_python_code", "")
            pyspark_code = chunk.get("chunk_pyspark_code", "")
            sas_code = chunk.get("chunk_sas_code", "")

            if python_code.strip():
                python_blocks.append(python_code)
            if pyspark_code.strip():
                pyspark_blocks.append(pyspark_code)
            if sas_code.strip():
                sas_blocks.append(sas_code)

        unique_python_blocks = list(set(python_blocks))
        unique_pyspark_blocks = list(set(pyspark_blocks))
        unique_sas_blocks = list(set(sas_blocks))

        now = datetime.now()

        state.merged_python_code = (
            "# Python Code Generated from SAS Translation\n"
            f"# Generated on: {now}\n\n" +
            "\n\n".join(unique_python_blocks)
        )

        state.merged_pyspark_code = (
            "# PySpark Code Generated from SAS Translation\n"
            f"# Generated on: {now}\n\n" +
            "\n\n".join(unique_pyspark_blocks)
        )

        state.merged_sas_code = (
            "# Original SAS Code\n"
            f"# Generated on: {now}\n\n" +
            "\n\n".join(unique_sas_blocks)
        )

        state.python_blocks_count = len(unique_python_blocks)
        state.pyspark_blocks_count = len(unique_pyspark_blocks)
        state.sas_blocks_count = len(unique_sas_blocks)
        state.total_chunks_processed = len(state.translated_chunks)

    except Exception as e:
        print(f"Error during code merging: {str(e)}")
        state.merged_sas_code = "# Error in code merging"
        state.merged_python_code = "# Error in code merging"
        state.merged_pyspark_code = "# Error in code merging"
        state.error_step = "merge"
        state.error_detail = str(e)

    return state

def validate_python_node(state: SASPipelineState) -> SASPipelineState:
    
    print("[NODE] validate_python_code\nValidating Python syntax...")

    code = state.merged_python_code
    try:
        # Parse code
        try:
            ast.parse(code)
            syntax_valid = True
            syntax_errors = []
        except SyntaxError as e:
            syntax_valid = False
            syntax_errors = [{
                "line": e.lineno,
                "column": e.offset,
                "message": e.msg
            }]
        except Exception as e:
            syntax_valid = False
            syntax_errors = [{"message": str(e)}]

        # Warnings
        warnings = []
        imported_modules = re.findall(r'import\s+(\w+)', code)
        if 'pandas' in code and not any(mod in ['pandas', 'pd'] for mod in imported_modules):
            warnings.append("pandas usage detected but not imported")
        if 'numpy' in code and not any(mod in ['numpy', 'np'] for mod in imported_modules):
            warnings.append("numpy usage detected but not imported")
        if 'spark' in code and 'pyspark' not in code:
            warnings.append("PySpark usage detected but pyspark not imported")

        # Update state
        state.python_validation = {
            "syntax_valid": syntax_valid,
            "syntax_errors": syntax_errors,
            "warnings": warnings,
            "code_length": len(code),
            "line_count": len(code.splitlines())
        }

    except Exception as e:
        state.python_validation = {
            "syntax_valid": False,
            "error": str(e),
            "syntax_errors": [{"message": f"Validation error: {str(e)}"}]
        }
        state.error_step = "python_validation"
        state.error_detail = str(e)

    return state

def validate_pyspark_node(state: SASPipelineState) -> SASPipelineState:
    print("[NODE] Validate pyspark code \n Validating PySpark syntax...")

    code = state.merged_pyspark_code
    try:
        try:
            ast.parse(code)
            basic_syntax_valid = True
            syntax_errors = []
        except SyntaxError as e:
            basic_syntax_valid = False
            syntax_errors = [{
                "line": e.lineno,
                "column": e.offset,
                "message": e.msg
            }]
        except Exception as e:
            basic_syntax_valid = False
            syntax_errors = [{"message": str(e)}]

        # Warnings and pattern detection
        warnings = []
        detected_patterns = []
        pyspark_patterns = [
            (r'spark\.', "spark object usage"),
            (r'\.read\.', "DataFrameReader usage"),
            (r'\.write\.', "DataFrameWriter usage"),
            (r'\.select\(', "select operation"),
            (r'\.filter\(', "filter operation"),
            (r'\.groupBy\(', "groupBy operation"),
            (r'\.agg\(', "aggregation operation"),
            (r'\.join\(', "join operation")
        ]

        for pattern, description in pyspark_patterns:
            if re.search(pattern, code):
                detected_patterns.append(description)

        if any(kw in code for kw in ['spark.', 'SparkSession', 'DataFrame']):
            if 'pyspark' not in code and 'SparkSession' not in code:
                warnings.append("PySpark usage detected but SparkSession not properly initialized")

        # Update state
        state.pyspark_validation = {
            "basic_syntax_valid": basic_syntax_valid,
            "syntax_errors": syntax_errors,
            "warnings": warnings,
            "detected_pyspark_patterns": detected_patterns,
            "code_length": len(code),
            "line_count": len(code.splitlines())
        }

    except Exception as e:
        state.pyspark_validation = {
            "basic_syntax_valid": False,
            "error": str(e),
            "syntax_errors": [{"message": f"Validation error: {str(e)}"}]
        }
        state.error_step = "pyspark_validation"
        state.error_detail = str(e)

    return state

# --- Graph ---
graph = StateGraph(SASPipelineState)
graph.add_node("chunk_node", chunk_sas_script_node)
graph.add_node("ast_node", extract_ast_node)
graph.add_node("translate_node", translate_chunk_node)
graph.add_node("merge_node", merge_code_node)
graph.add_node("validate_python", validate_python_node)
graph.add_node("validate_pyspark", validate_pyspark_node)

graph.add_edge(START, "chunk_node")
graph.add_edge("chunk_node", "ast_node")
graph.add_edge("ast_node", "translate_node")
graph.add_edge("translate_node", "merge_node")
graph.add_edge("merge_node", "validate_python")
graph.add_edge("validate_python", "validate_pyspark")
graph.add_edge("validate_pyspark", END)

pipeline = graph.compile()

#---Test the sample script----

# query= """Analysis script"""
# # from IPython.display import Image
# # Image(pipeline.get_graph().draw_mermaid_png())


# init_state = SASPipelineState(
#     file_path=None,
#     script_content=query,
#     chunk_size=100
# )
# raw = pipeline.invoke(init_state,stream_mode="debug",config={"configurable": {"thread_id": "1"}})
# final = SASPipelineState(**raw)

# # Inspect final state
# print("Chunks:",final.chunks)
# print(f"DEBUG ==>> LEN : {len(final.chunks)}")
# print("AST list length:",         len(final.chunks_with_ast))
# print("Translated chunks:",       len(final.translated_chunks))
# print("Merged Python code length:", len(final.merged_python_code))
# print("Python syntax valid:",     final.python_validation.get("syntax_valid"))
# print("PySpark syntax valid:",    final.pyspark_validation.get("basic_syntax_valid"))
# if final.error_step:
#     print("Error at step:", final.error_step)
#     print("Error detail:", final.error_detail)


# ##Testing the graph streaming here:: 

# for chunk in pipeline.stream(init_state, stream_mode="values",config={"configurable": {"thread_id": "1"}}):
#     print(f"Type of the chunk {i+1} : {type(chunk)}")
    
#     print(chunk)