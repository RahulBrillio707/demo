"""
SAS Parser - Converts SAS lexer tokens into structured YAML schema format
"""
import uuid
from typing import List, Dict, Any, Optional, Union
from enum import Enum
from sas_lexer import lex_program_from_str
import json
from pyvis.network import Network


# Comprehensive TokenType class mapped from YAML schema requirements
class TokenType:
    # ========== CORE TOKENS ==========
    EOF = 0
    MACRO_SEP = 1
    CATCH_ALL = 2
    WS = 3
    SEMI = 4
    AMP = 5
    PERCENT = 6
    LPAREN = 7
    RPAREN = 8
    LCURLY = 9
    RCURLY = 10
    LBRACK = 11
    RBRACK = 12
    
    # ========== OPERATORS ==========
    STAR = 13
    EXCL = 14
    PLUS = 22
    MINUS = 23
    LT = 26
    LE = 27
    NE = 28
    GT = 29
    GE = 30
    PIPE = 32
    DOT = 33
    COMMA = 34
    COLON = 35
    ASSIGN = 36
    DOLLAR = 37
    AT = 38
    HASH = 39
    QUESTION = 40
    
    # ========== COMPARISON KEYWORDS ==========
    KW_LT = 41
    KW_LE = 42
    KW_EQ = 43
    KW_IN = 44
    KW_NE = 45
    KW_GT = 46
    KW_GE = 47
    KW_AND = 48
    KW_OR = 49
    KW_NOT = 50
    
    # ========== LITERALS ==========
    INTEGER_LITERAL = 51
    FLOAT_LITERAL = 52
    FLOAT_EXPONENT_LITERAL = 53
    STRING_LITERAL = 54
    BIT_TESTING_LITERAL = 55
    DATE_LITERAL = 56
    DATE_TIME_LITERAL = 57
    NAME_LITERAL = 58
    TIME_LITERAL = 59
    HEX_STRING_LITERAL = 60
    
    # ========== COMMENTS ==========
    C_STYLE_COMMENT = 70
    PREDICTED_COMMENT_STAT = 71
    MACRO_COMMENT = 75
    
    # ========== DATALINES ==========
    DATALINES_START = 72
    DATALINES_DATA = 73
    
    # ========== MACRO TOKENS ==========
    MACRO_VAR_RESOLVE = 76
    MACRO_VAR_TERM = 77
    MACRO_STRING = 78
    MACRO_STRING_EMPTY = 79
    MACRO_LABEL = 80
    MACRO_IDENTIFIER = 81
    
    # ========== MACRO FUNCTIONS ==========
    KWM_CMPRES = 82
    KWM_EVAL = 85
    KWM_SYSFUNC = 96
    KWM_TRIM = 103
    KWM_UNQUOTE = 104
    KWM_UPCASE = 105
    
    # ========== MACRO CONTROL STATEMENTS ==========
    KWM_ABORT = 140
    KWM_DO = 143
    KWM_TO = 144
    KWM_BY = 145
    KWM_UNTIL = 146
    KWM_WHILE = 147
    KWM_END = 148
    KWM_GLOBAL = 149
    KWM_IF = 151
    KWM_THEN = 152
    KWM_ELSE = 153
    KWM_INPUT = 154
    KWM_LET = 155
    KWM_LOCAL = 156
    KWM_MACRO = 157
    KWM_MEND = 158
    KWM_PUT = 159
    KWM_RETURN = 160
    KWM_INCLUDE = 169
    KWM_LIST = 170
    KWM_RUN = 171
    
    # ========== IDENTIFIERS ==========
    IDENTIFIER = 172
    
    # ========== GLOBAL STATEMENTS ==========
    KW_LIBNAME = 179
    KW_FILENAME = 180
    KW_CLEAR = 181
    KW_LIST = 182
    KW_CANCEL = 183
    
    # ========== DATA STEP KEYWORDS ==========
    KW_ALLVAR = 184
    KW_ARRAY = 185
    KW_ATTRIB = 186
    KW_CALL = 187
    KW_DATA = 188
    KW_DEFAULT = 189
    KW_DESCENDING = 190
    KW_FORMAT = 191
    KW_GROUPFORMAT = 192
    KW_ID = 193
    KW_IF = 194
    KW_INFILE = 195
    KW_INFORMAT = 196
    KW_KEEP = 197
    KW_LABEL = 198
    KW_LENGTH = 199
    KW_MERGE = 200
    KW_NULL_DATASET = 201
    KW_OUTPUT = 202
    KW_PGM = 203
    KW_RENAME = 204
    KW_RUN = 205
    KW_SET = 206
    KW_STOP = 207
    KW_VAR = 208
    KW_VIEW = 209
    KW_WITH = 210
    KW_DELETE = 211
    KW_NOTSORTED = 212
    
    # ========== PROC KEYWORDS ==========
    KW_PROC = 213
    KW_QUIT = 214
    KW_RANKS = 215
    
    # ========== SQL KEYWORDS ==========
    KW_ALL = 216
    KW_ANY = 217
    KW_AS = 218
    KW_ASC = 219
    KW_BETWEEN = 220
    KW_BY = 223
    KW_CALCULATED = 224
    KW_CASE = 225
    KW_CONNECT = 226
    KW_CONNECTION = 227
    KW_CONTAINS = 228
    KW_CREATE = 230
    KW_CROSS = 231
    KW_DESC = 232
    KW_DISCONNECT = 233
    KW_DISTINCT = 234
    KW_DO = 235
    KW_DROP = 236
    KW_ELSE = 237
    KW_END = 238
    KW_EXCEPT = 240
    KW_EXECUTE = 241
    KW_EXISTS = 242
    KW_FOR = 243
    KW_FROM = 244
    KW_FULL = 245
    KW_GROUP = 246
    KW_HAVING = 247
    KW_INDEX = 248
    KW_INNER = 249
    KW_INSERT = 250
    KW_INTERSECT = 251
    KW_INTO = 252
    KW_IS = 253
    KW_JOIN = 254
    KW_LEFT = 257
    KW_LIKE = 258
    KW_MISSING = 259
    KW_NULL = 262
    KW_ON = 263
    KW_ORDER = 264
    KW_OUTER = 265
    KW_PRIMARY = 266
    KW_RIGHT = 267
    KW_SELECT = 268
    KW_TABLE = 271
    KW_THEN = 272
    KW_TO = 273
    KW_UNION = 276
    KW_UNIQUE = 277
    KW_UPDATE = 278
    KW_USING = 279
    KW_VALUES = 280
    KW_WHEN = 281
    KW_WHERE = 282
    KW_INPUT = 286
    KW_PUT = 287


class TokenChannel(Enum):
    DEFAULT = 0
    HIDDEN = 1
    COMMENT = 2


class SasParser:
    """
    Main SAS parser that converts tokens into YAML schema format
    """
    
    def __init__(self, tokens: List, source_code: str, filename: str = ""):
        self.all_tokens = tokens
        self.source = source_code
        self.filename = filename
        
        # Optimized: Single pass token filtering instead of 3 passes
        self.main_tokens = []
        self.comment_tokens = []
        self.hidden_tokens = []
        
        for token in tokens:
            if token.channel == TokenChannel.DEFAULT.value:
                self.main_tokens.append(token)
            elif token.channel == TokenChannel.COMMENT.value:
                self.comment_tokens.append(token)
            else:
                self.hidden_tokens.append(token)
        
        self.pos = 0
        self.current_level = 0
        self.node_id_counter = 0
        
        # Add parsing context stack for proper nesting
        self.parsing_stack = []

    def push_parsing_context(self, context_type: str, expected_end_token: str):
        """Push a new parsing context onto the stack"""
        context = {
            'type': context_type,
            'expected_end': expected_end_token,
            'start_pos': self.pos,
            'level': self.current_level
        }
        self.parsing_stack.append(context)
        print(f"[DEBUG] Pushed {context_type} context, expecting {expected_end_token} at pos {self.pos}")
    
    def pop_parsing_context(self, found_token: str) -> bool:
        """Pop and validate parsing context"""
        if not self.parsing_stack:
            print(f"[DEBUG] No context to pop for token {found_token} at pos {self.pos}")
            return False
            
        context = self.parsing_stack[-1]
        if context['expected_end'] == found_token:
            self.parsing_stack.pop()
            print(f"[DEBUG] Correctly matched {context['type']} with {found_token} at pos {self.pos}")
            return True
        else:
            print(f"[WARNING] Expected {context['expected_end']} for {context['type']} but found {found_token} at pos {self.pos}")
            return False
    
    def peek_parsing_context(self) -> dict:
        """Get current parsing context without removing it"""
        return self.parsing_stack[-1] if self.parsing_stack else None

    def generate_id(self) -> str:
        """Generate unique ID for nodes"""
        self.node_id_counter += 1
        return f"node_{self.node_id_counter:06d}"

    def current_token(self):
        """Get current token or EOF"""
        if self.pos >= len(self.main_tokens):
            return None
        return self.main_tokens[self.pos]
    
    def peek_token(self, offset: int = 1):
        """Look ahead at token"""
        idx = self.pos + offset
        if idx >= len(self.main_tokens):
            return None
        return self.main_tokens[idx]
    
    def advance(self):
        """Move to next token"""
        if self.pos < len(self.main_tokens):
            self.pos += 1
    
    def at_end(self) -> bool:
        """Check if at end of tokens"""
        return self.pos >= len(self.main_tokens) or \
               (self.current_token() and self.current_token().token_type == TokenType.EOF)
    
    def match_token_type(self, token_type) -> bool:
        """Check if current token matches type"""
        token = self.current_token()
        return token and token.token_type == token_type
    
    def consume_token_type(self, token_type: TokenType, error_msg: str = ""):
        """Consume token of expected type or raise error"""
        if not self.match_token_type(token_type):
            token = self.current_token()
            actual = token.token_type if token else "EOF"
            raise SyntaxError(f"{error_msg}. Expected {token_type.name}, got {actual}")
        
        token = self.current_token()
        self.advance()
        return token
    
    def get_token_text(self, token) -> str:
        """Extract text from source using token positions"""
        if not token:
            return ""
        return self.source[token.start:token.stop]
    
    def create_base_node(self, start_token, end_token=None) -> Dict[str, Any]:
        """Create BaseNode fields from token positions"""
        if not start_token:
            raise ValueError("start_token required for base node")
            
        end_token = end_token or start_token
        
        base_node = {
            'id': self.generate_id(),
            'level': self.current_level,
            'location': {
                'file': self.filename,
                'line_start': start_token.line,
                'col_start': start_token.column,
                'line_end': end_token.end_line,
                'col_end': end_token.end_column,
                'byte_start': start_token.start,
                'byte_end': end_token.stop
            }
        }
        
        # Add comments if any are near this location
        comments = self.collect_comments_near_location(start_token, end_token)
        if comments:
            base_node['comments'] = comments
            
        return base_node
    
    def collect_comments_near_location(self, start_token, end_token) -> List[str]:
        """Collect comments that appear near this token range"""
        # Early exit if no comments (your file has only 11 comments, so this helps)
        if not self.comment_tokens:
            return []
            
        comments = []
        start_byte = start_token.start
        end_byte = end_token.stop
        
        for comment_token in self.comment_tokens:
            # Only include comments that are actually within or immediately before this construct
            if (comment_token.start >= start_byte - 50 and  # Allow small distance before
                comment_token.stop <= end_byte + 10):       # Allow small distance after
                comment_text = self.get_token_text(comment_token)
                comments.append(comment_text)
        
        return comments
        return []
    
    def skip_to_next_statement(self):
        """Skip tokens until next statement boundary"""
        # Pre-compute boundary token types for faster lookup
        boundary_types = {
            TokenType.SEMI, TokenType.KW_DATA, TokenType.KW_PROC, 
            TokenType.KWM_MACRO, TokenType.KWM_MEND, TokenType.KW_RUN,
            TokenType.KW_QUIT, TokenType.KW_LIBNAME, TokenType.KW_FILENAME,
            TokenType.KWM_LET, TokenType.KWM_INCLUDE, TokenType.PERCENT
        }
        
        while not self.at_end():
            token = self.current_token()
            if token and any(token.token_type == bt for bt in boundary_types):
                break
            self.advance()
    
    def parse_module(self) -> Dict[str, Any]:
        """Parse entire SAS module (top level)"""
        import time
        print(f"[DEBUG] parse_module started at pos {self.pos}/{len(self.main_tokens)}")
        
        # Find the span of the entire module using ALL tokens (including comments)
        first_token = self.all_tokens[0] if self.all_tokens else None
        last_token = self.all_tokens[-1] if self.all_tokens else first_token
        
        # Create mock token for empty files
        if not first_token:
            first_token = type('MockToken', (), {
                'line': 1, 'column': 0, 'end_line': 1, 'end_column': 0, 
                'start': 0, 'stop': 0
            })()
            last_token = first_token
        
        module_node = self.create_base_node(first_token, last_token)
        module_node.update({
            'type': 'SasModule',
            'encoding': None,
            'includes': [],
            'options_snapshot': {},
            'symbol_table': {},
            'body': []
        })
        
        # Parse top-level constructs with safety counter
        iteration_count = 0
        max_iterations = len(self.main_tokens) + 1000  # Safety limit
        
        while not self.at_end():
            iteration_count += 1
            old_pos = self.pos
            current_token = self.current_token()
            
            # More frequent logging near the problem area
            if self.pos >= 2430 or iteration_count % 10 == 0:
                token_text = self.get_token_text(current_token) if current_token else "None"
                token_type = current_token.token_type if current_token else "None"
                print(f"[DEBUG] Iteration {iteration_count}, pos {self.pos}/{len(self.main_tokens)}, token: '{token_text}', type: {token_type}")
            
            if iteration_count > max_iterations:
                print(f"[ERROR] Parse loop exceeded {max_iterations} iterations, breaking")
                break
            
            try:
                if self.match_token_type(TokenType.KW_DATA):
                    print(f"[DEBUG] Parsing DATA step at pos {self.pos}")
                    module_node['body'].append(self.parse_data_step())
                elif self.match_token_type(TokenType.KW_PROC):
                    print(f"[DEBUG] Parsing PROC step at pos {self.pos}")
                    module_node['body'].append(self.parse_proc_step())
                elif self.match_token_type(TokenType.KWM_MACRO):
                    print(f"[DEBUG] Parsing MACRO def at pos {self.pos}")
                    module_node['body'].append(self.parse_macro_def())
                elif self.match_token_type(TokenType.KWM_LET):
                    print(f"[DEBUG] Parsing LET statement at pos {self.pos}")
                    module_node['body'].append(self.parse_let_statement())
                elif self.match_token_type(TokenType.KWM_INCLUDE):
                    print(f"[DEBUG] Parsing INCLUDE statement at pos {self.pos}")
                    module_node['body'].append(self.parse_include_statement())
                elif self.match_token_type(TokenType.KW_LIBNAME):
                    print(f"[DEBUG] Parsing LIBNAME statement at pos {self.pos}")
                    module_node['body'].append(self.parse_libname_statement())
                elif self.match_token_type(TokenType.KW_FILENAME):
                    print(f"[DEBUG] Parsing FILENAME statement at pos {self.pos}")
                    module_node['body'].append(self.parse_filename_statement())
                elif self.match_token_type(TokenType.PERCENT):
                    print(f"[DEBUG] Parsing macro call at pos {self.pos}")
                    # Check if this is a macro call
                    macro_call = self.parse_macro_call()
                    if macro_call:
                        module_node['body'].append(macro_call)
                    else:
                        unknown_node = self.parse_unknown_statement()
                        if unknown_node:
                            module_node['body'].append(unknown_node)
                elif self.match_token_type(TokenType.MACRO_IDENTIFIER):  # Add this new condition
                    print(f"[DEBUG] Parsing MACRO_IDENTIFIER as macro call at pos {self.pos}")
                    macro_call = self.parse_macro_identifier_call()
                    if macro_call:
                        module_node['body'].append(macro_call)
                    else:
                        unknown_node = self.parse_unknown_statement()
                        if unknown_node:
                            module_node['body'].append(unknown_node)
                elif self.match_token_type(TokenType.KW_FORMAT):
                    print(f"[DEBUG] Parsing FORMAT statement at module level at pos {self.pos}")
                    module_node['body'].append(self.parse_format_statement())
                elif self.match_token_type(TokenType.IDENTIFIER):
                    current_token = self.current_token()
                    token_text = self.get_token_text(current_token) if current_token else "None"
                    print(f"[DEBUG] Checking IDENTIFIER '{token_text}' for assignment at module level at pos {self.pos}")
                    
                    # Check if this is an assignment statement by looking ahead for assignment operator
                    # Pattern: IDENTIFIER + MACRO_VAR_RESOLVE + MACRO_STRING + ASSIGN (for NumSearches_&type=0;)
                    lookahead_pos = 1
                    found_assignment = False
                    
                    # Look ahead to find assignment operator, accounting for macro variables
                    while lookahead_pos <= 10:  # Reasonable lookahead limit
                        lookahead_token = self.peek_token(lookahead_pos)
                        if not lookahead_token:
                            break
                        
                        lookahead_type = lookahead_token.token_type
                        lookahead_text = self.get_token_text(lookahead_token)
                        print(f"[DEBUG] Module level lookahead {lookahead_pos}: type={lookahead_type}, text='{lookahead_text}'")
                        
                        if lookahead_type == TokenType.ASSIGN:
                            found_assignment = True
                            print(f"[DEBUG] Found assignment operator at module level lookahead position {lookahead_pos}")
                            break
                        elif lookahead_type == TokenType.SEMI:
                            print(f"[DEBUG] Found semicolon at module level lookahead position {lookahead_pos}, no assignment")
                            break  # End of statement without assignment
                        elif lookahead_type in [TokenType.MACRO_VAR_RESOLVE, TokenType.MACRO_STRING, TokenType.WS]:
                            # These are expected parts of a macro variable, continue looking
                            print(f"[DEBUG] Skipping macro/whitespace token at module level lookahead {lookahead_pos}")
                            pass
                        else:
                            # Some other token type that shouldn't be in a variable name
                            print(f"[DEBUG] Unexpected token type {lookahead_type} at module level lookahead {lookahead_pos}, stopping")
                            break
                        lookahead_pos += 1
                    
                    if found_assignment:
                        print(f"[DEBUG] Parsing assignment statement at module level at pos {self.pos}")
                        module_node['body'].append(self.parse_macro_assignment_statement())
                    else:
                        print(f"[DEBUG] IDENTIFIER '{token_text}' not assignment at module level, parsing as unknown")
                        unknown_node = self.parse_unknown_statement()
                        if unknown_node:
                            module_node['body'].append(unknown_node)
                else:
                    print(f"[DEBUG] Parsing unknown statement at pos {self.pos}")
                    # Unknown statement - create SasUnknownStatement
                    unknown_node = self.parse_unknown_statement()
                    if unknown_node:
                        module_node['body'].append(unknown_node)
                
                # Check if position advanced
                if self.pos == old_pos:
                    token_text = self.get_token_text(current_token) if current_token else "None"
                    print(f"[ERROR] Parser stuck at position {self.pos}, token: '{token_text}', type: {current_token.token_type if current_token else 'None'}")
                    print(f"[DEBUG] Forcing advance from pos {self.pos}")
                    self.advance()  # Force advance to prevent infinite loop
                    print(f"[DEBUG] Advanced to pos {self.pos}")
                        
            except Exception as e:
                print(f"[ERROR] Exception at pos {self.pos}: {e}")
                # Error recovery - skip to next statement
                error_token = self.current_token()
                error_node = self.create_base_node(error_token) if error_token else {}
                error_node.update({
                    'type': 'SasUnknownStatement',
                    'kind': 'error',
                    'text': f"Parse error: {str(e)}",
                    'errors': [str(e)]
                })
                module_node['body'].append(error_node)
                self.skip_to_next_statement()
        
        print(f"[DEBUG] parse_module completed. Body items: {len(module_node['body'])}")
        return module_node
    
    def parse_unknown_statement(self) -> Optional[Dict[str, Any]]:
        """Parse unknown/unrecognized statement"""
        start_token = self.current_token()
        if not start_token:
            return None
            
        print(f"[DEBUG] parse_unknown_statement at pos {self.pos}, token: {self.get_token_text(start_token)}")
        
        # Collect tokens until semicolon or end with safety limit
        tokens_text = []
        safety_counter = 0
        max_tokens_in_statement = 500  # Safety limit
        
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI) and
               safety_counter < max_tokens_in_statement):
            token = self.current_token()
            tokens_text.append(self.get_token_text(token))
            self.advance()
            safety_counter += 1
        
        if safety_counter >= max_tokens_in_statement:
            print(f"[WARNING] parse_unknown_statement hit safety limit at pos {self.pos}")
        
        # Include semicolon if present
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            tokens_text.append(self.get_token_text(end_token))
            self.advance()
        
        unknown_node = self.create_base_node(start_token, end_token)
        unknown_node.update({
            'type': 'SasUnknownStatement',
            'kind': 'unknown',
            'text': ''.join(tokens_text)
        })
        
        print(f"[DEBUG] parse_unknown_statement completed, advanced to pos {self.pos}")
        return unknown_node
    
    def parse_data_step(self) -> Dict[str, Any]:
        """Parse DATA step"""
        start_token = self.current_token()
        self.advance()  # consume 'data'
        
        # Parse dataset name(s)
        dataset_names = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI)):
            if self.match_token_type(TokenType.IDENTIFIER):
                dataset_names.append(self.get_token_text(self.current_token()))
                self.advance()
            else:
                self.advance()  # skip non-identifier tokens
        
        # Consume semicolon
        if self.match_token_type(TokenType.SEMI):
            self.advance()
        
        # Parse statements until RUN
        statements = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.KW_RUN)):
            
            stmt = None
            if self.match_token_type(TokenType.KW_SET):
                stmt = self.parse_set_statement()
            elif self.match_token_type(TokenType.KW_MERGE):
                stmt = self.parse_merge_statement()
            elif self.match_token_type(TokenType.KW_UPDATE):
                stmt = self.parse_update_statement()
            elif self.match_token_type(TokenType.KW_IF):
                stmt = self.parse_if_statement()
            elif self.match_token_type(TokenType.KW_OUTPUT):
                stmt = self.parse_output_statement()
            elif self.match_token_type(TokenType.IDENTIFIER):
                # Check if this is an assignment statement
                next_token = self.peek_token()
                if next_token and next_token.token_type == TokenType.ASSIGN:
                    stmt = self.parse_assignment_statement()
                else:
                    # Unknown statement - skip
                    stmt = self.parse_unknown_statement()
            else:
                # Unknown statement - skip until semicolon
                stmt = self.parse_unknown_statement()
            
            if stmt:
                statements.append(stmt)
        
        # Find RUN; statement
        end_token = start_token
        if self.match_token_type(TokenType.KW_RUN):
            run_token = self.current_token()
            self.advance()  # consume 'run'
            if self.match_token_type(TokenType.SEMI):
                end_token = self.current_token()
                self.advance()  # consume ';'
        
        data_node = self.create_base_node(start_token, end_token)
        data_node.update({
            'type': 'SasDataStep',
            'name': ' '.join(dataset_names) if dataset_names else 'unnamed',
            'datasets_out': dataset_names,
            'statements': statements
        })
        
        return data_node
    
    def parse_proc_step(self) -> Dict[str, Any]:
        """Parse PROC step"""
        start_token = self.current_token()
        self.advance()  # consume 'proc'
        
        # Get procedure name - check for both IDENTIFIER and SQL keyword
        proc_name = "unknown"
        current = self.current_token()
        if current:
            if self.match_token_type(TokenType.IDENTIFIER):
                proc_name = self.get_token_text(current)
                self.advance()
            elif current.token_type == TokenType.KW_SELECT:  # SQL as special case
                proc_name = "sql"
                # Don't advance - SQL is part of PROC SQL statements
            elif current.token_type in [TokenType.KW_FROM, TokenType.KW_WHERE, TokenType.KW_CREATE]:  # Other SQL keywords
                proc_name = "sql"
            else:
                # Try to extract text for any token type
                proc_name = self.get_token_text(current)
                self.advance()
        
        # Parse options until semicolon
        options = {}
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI)):
            if self.match_token_type(TokenType.IDENTIFIER):
                option_name = self.get_token_text(self.current_token())
                self.advance()
                if self.match_token_type(TokenType.ASSIGN):
                    self.advance()  # consume '='
                    if self.match_token_type(TokenType.IDENTIFIER):
                        option_value = self.get_token_text(self.current_token())
                        options[option_name] = option_value
                        self.advance()
            else:
                self.advance()
        
        # Consume semicolon
        if self.match_token_type(TokenType.SEMI):
            self.advance()
        
        # Parse statements until RUN/QUIT - enhanced for PROC SQL
        statements = []
        proc_iteration_count = 0
        max_proc_iterations = 1000  # Safety limit
        
        print(f"[DEBUG] Starting PROC {proc_name} statement parsing at pos {self.pos}")
        
        while (not self.at_end() and 
               not self.match_token_type(TokenType.KW_RUN) and
               not self.match_token_type(TokenType.KW_QUIT) and
               proc_iteration_count < max_proc_iterations):
            
            proc_iteration_count += 1
            old_pos = self.pos
            
            if proc_iteration_count % 10 == 0 or self.pos >= 2440:
                current_token = self.current_token()
                token_text = self.get_token_text(current_token) if current_token else "None"
                print(f"[DEBUG] PROC {proc_name} iteration {proc_iteration_count}, pos {self.pos}, token: '{token_text}'")
            
            # Special handling for PROC SQL
            if proc_name.lower() == "sql":
                print(f"[DEBUG] Calling parse_proc_sql_statement for SQL at pos {self.pos}")
                sql_stmt = self.parse_proc_sql_statement()
                if sql_stmt:
                    statements.append(sql_stmt)
                    print(f"[DEBUG] SQL statement parsed successfully, now at pos {self.pos}")
                else:
                    print(f"[DEBUG] No SQL statement found, skipping to next at pos {self.pos}")
                    self.skip_to_next_statement()
                    if self.match_token_type(TokenType.SEMI):
                        self.advance()
            else:
                # Parse other PROC statements
                if self.match_token_type(TokenType.KW_BY):
                    statements.append(self.parse_proc_by_statement())
                elif self.match_token_type(TokenType.KW_VAR):
                    statements.append(self.parse_proc_var_statement())
                else:
                    # Skip other statements for now
                    self.skip_to_next_statement()
                    if self.match_token_type(TokenType.SEMI):
                        self.advance()
            
            # Safety check for infinite loops
            if self.pos == old_pos:
                print(f"[ERROR] PROC parser stuck at position {self.pos}, forcing advance")
                self.advance()
                
        if proc_iteration_count >= max_proc_iterations:
            print(f"[ERROR] PROC parsing exceeded {max_proc_iterations} iterations, breaking")
        
        # Find RUN; or QUIT; statement
        end_token = start_token
        if self.match_token_type(TokenType.KW_RUN) or self.match_token_type(TokenType.KW_QUIT):
            run_token = self.current_token()
            self.advance()  # consume 'run' or 'quit'
            if self.match_token_type(TokenType.SEMI):
                end_token = self.current_token()
                self.advance()  # consume ';'
        
        proc_node = self.create_base_node(start_token, end_token)
        proc_node.update({
            'type': 'SasProcStep',
            'procedure_name': proc_name,
            'options': options,
            'statements': statements
        })
        
        return proc_node
    
    def parse_macro_def(self) -> Dict[str, Any]:
        """Parse macro definition"""
        start_token = self.current_token()
        self.advance()  # consume '%macro'
        
        # Get macro name
        macro_name = "unknown"
        if self.match_token_type(TokenType.IDENTIFIER):
            macro_name = self.get_token_text(self.current_token())
            self.advance()
        
        # Parse parameters if present
        parameters = []
        if self.match_token_type(TokenType.LPAREN):
            self.advance()  # consume '('
            
            # Parse parameter list
            while (not self.at_end() and 
                   not self.match_token_type(TokenType.RPAREN)):
                
                # Skip whitespace and commas
                if self.match_token_type(TokenType.WS) or self.match_token_type(TokenType.COMMA):
                    self.advance()
                    continue
                
                # Get parameter name
                if self.match_token_type(TokenType.IDENTIFIER):
                    param_name = self.get_token_text(self.current_token())
                    
                    # Create parameter object with default value support
                    param = {
                        'name': param_name,
                        'default_value': None
                    }
                    
                    self.advance()  # consume parameter name
                    
                    # Check for default value (param=default)
                    if self.match_token_type(TokenType.ASSIGN):
                        self.advance()  # consume '='
                        
                        # Get default value
                        if (self.match_token_type(TokenType.IDENTIFIER) or
                            self.match_token_type(TokenType.STRING_LITERAL) or
                            self.match_token_type(TokenType.INTEGER_LITERAL)):
                            param['default_value'] = self.get_token_text(self.current_token())
                            self.advance()
                    
                    parameters.append(param)
                else:
                    # Skip unknown tokens
                    self.advance()
            
            # Consume closing parenthesis
            if self.match_token_type(TokenType.RPAREN):
                self.advance()
        
        # Skip any remaining tokens until semicolon
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI)):
            self.advance()
        
        # Consume semicolon
        if self.match_token_type(TokenType.SEMI):
            self.advance()
        
        # Parse macro body (increment level for nested constructs)  
        self.current_level += 1
        macro_body = []
        
        # Push macro context onto parsing stack
        self.push_parsing_context("MACRO", "%MEND")
        
        macro_iteration_count = 0
        max_macro_iterations = len(self.main_tokens) + 500
        
        while (not self.at_end() and 
               not self.match_token_type(TokenType.KWM_MEND)):
            
            macro_iteration_count += 1
            old_pos = self.pos
            current_token = self.current_token()
            
            # Enhanced logging near problem area
            if self.pos >= 2430 or macro_iteration_count % 10 == 0:
                token_text = self.get_token_text(current_token) if current_token else "None"
                token_type = current_token.token_type if current_token else "None"
                print(f"[DEBUG] Macro body iteration {macro_iteration_count}, pos {self.pos}/{len(self.main_tokens)}, token: '{token_text}', type: {token_type}")
            
            if macro_iteration_count > max_macro_iterations:
                print(f"[ERROR] Macro body parsing exceeded {max_macro_iterations} iterations, breaking")
                break
                
            if self.match_token_type(TokenType.KW_DATA):
                print(f"[DEBUG] Parsing DATA step in macro at pos {self.pos}")
                macro_body.append(self.parse_data_step())
            elif self.match_token_type(TokenType.KW_PROC):
                print(f"[DEBUG] Parsing PROC step in macro at pos {self.pos}")
                macro_body.append(self.parse_proc_step())
            elif self.match_token_type(TokenType.KWM_LET):
                print(f"[DEBUG] Parsing LET statement in macro at pos {self.pos}")
                macro_body.append(self.parse_let_statement())
            elif self.match_token_type(TokenType.KWM_INCLUDE):
                print(f"[DEBUG] Parsing INCLUDE statement in macro at pos {self.pos}")
                macro_body.append(self.parse_include_statement())
            elif self.match_token_type(TokenType.KW_LIBNAME):
                print(f"[DEBUG] Parsing LIBNAME statement in macro at pos {self.pos}")
                macro_body.append(self.parse_libname_statement())
            elif self.match_token_type(TokenType.KW_FILENAME):
                print(f"[DEBUG] Parsing FILENAME statement in macro at pos {self.pos}")
                macro_body.append(self.parse_filename_statement())
            elif self.match_token_type(TokenType.KWM_IF):
                print(f"[DEBUG] Parsing macro IF statement at pos {self.pos}")
                macro_body.append(self.parse_macro_if_statement())
            elif self.match_token_type(TokenType.KWM_DO):
                print(f"[DEBUG] Parsing macro DO statement at pos {self.pos}")
                macro_body.append(self.parse_macro_do_statement())
            elif self.match_token_type(TokenType.KWM_MACRO):
                print(f"[DEBUG] Parsing nested MACRO definition at pos {self.pos}")
                macro_body.append(self.parse_macro_def())
            elif self.match_token_type(TokenType.PERCENT):
                print(f"[DEBUG] Parsing macro call in macro body at pos {self.pos}")
                # Check if this is a macro call
                macro_call = self.parse_macro_call()
                if macro_call:
                    macro_body.append(macro_call)
                else:
                    unknown_stmt = self.parse_unknown_statement()
                    if unknown_stmt:
                        macro_body.append(unknown_stmt)
            elif self.match_token_type(TokenType.MACRO_IDENTIFIER):
                print(f"[DEBUG] Parsing MACRO_IDENTIFIER as macro call in macro body at pos {self.pos}")
                macro_call = self.parse_macro_identifier_call()
                if macro_call:
                    macro_body.append(macro_call)
                else:
                    unknown_stmt = self.parse_unknown_statement()
                    if unknown_stmt:
                        macro_body.append(unknown_stmt)
            elif self.match_token_type(TokenType.KW_FORMAT):
                print(f"[DEBUG] Parsing FORMAT statement in macro body at pos {self.pos}")
                macro_body.append(self.parse_format_statement())
            elif self.match_token_type(TokenType.IDENTIFIER):
                current_token = self.current_token()
                identifier_text = self.get_token_text(current_token) if current_token else "None"
                print(f"[DEBUG] Checking IDENTIFIER '{identifier_text}' in macro body at pos {self.pos}")
                
                # First check if this is an assignment statement by looking ahead for assignment operator
                # Pattern: IDENTIFIER + MACRO_VAR_RESOLVE + MACRO_STRING + ASSIGN (for NumSearches_&type=0;)
                lookahead_pos = 1
                found_assignment = False
                
                # Look ahead to find assignment operator, accounting for macro variables
                while lookahead_pos <= 10:  # Reasonable lookahead limit
                    lookahead_token = self.peek_token(lookahead_pos)
                    if not lookahead_token:
                        break
                    
                    lookahead_type = lookahead_token.token_type
                    lookahead_text = self.get_token_text(lookahead_token)
                    print(f"[DEBUG] Macro body lookahead {lookahead_pos}: type={lookahead_type}, text='{lookahead_text}'")
                    
                    if lookahead_type == TokenType.ASSIGN:
                        found_assignment = True
                        print(f"[DEBUG] Found assignment operator in macro body at lookahead position {lookahead_pos}")
                        break
                    elif lookahead_type == TokenType.SEMI:
                        print(f"[DEBUG] Found semicolon in macro body at lookahead position {lookahead_pos}, no assignment")
                        break  # End of statement without assignment
                    elif lookahead_type in [TokenType.MACRO_VAR_RESOLVE, TokenType.MACRO_STRING, TokenType.WS]:
                        # These are expected parts of a macro variable, continue looking
                        print(f"[DEBUG] Skipping macro/whitespace token in macro body at lookahead {lookahead_pos}")
                        pass
                    else:
                        # Some other token type that shouldn't be in a variable name
                        print(f"[DEBUG] Unexpected token type {lookahead_type} in macro body at lookahead {lookahead_pos}, stopping")
                        break
                    lookahead_pos += 1
                
                if found_assignment:
                    print(f"[DEBUG] Parsing assignment statement in macro body at pos {self.pos}")
                    macro_body.append(self.parse_macro_assignment_statement())
                else:
                    # Check if this could be a bare macro call
                    # Look ahead for semicolon
                    if self.peek_token() and self.peek_token().token_type == TokenType.SEMI:
                        # Check if this identifier matches a known macro name
                        if self._is_known_macro_name(identifier_text):
                            print(f"[DEBUG] Parsing bare macro call '{identifier_text}' in macro body")
                            macro_call = self.parse_bare_macro_call()
                            if macro_call:
                                macro_body.append(macro_call)
                                continue
                    
                    # Fall through to unknown statement if not a macro call or assignment
                    print(f"[DEBUG] IDENTIFIER '{identifier_text}' not assignment or known macro in macro body, parsing as unknown")
                    unknown_stmt = self.parse_unknown_statement()
                    if unknown_stmt:
                        macro_body.append(unknown_stmt)
            elif self.match_token_type(TokenType.KW_END):
                print(f"[DEBUG] Handling regular END token in macro at pos {self.pos}")
                # Regular END statement - create simple statement and advance
                start_token = self.current_token()
                self.advance()  # consume 'END'
                
                # Consume semicolon if present
                end_token = start_token
                if self.match_token_type(TokenType.SEMI):
                    end_token = self.current_token()
                    self.advance()
                
                end_stmt = self.create_base_node(start_token, end_token)
                end_stmt.update({
                    'type': 'SasEndStatement',
                    'text': 'END'
                })
                macro_body.append(end_stmt)
            else:
                # Parse unknown macro statement with proper termination
                current_token = self.current_token()
                token_text = self.get_token_text(current_token) if current_token else "None"
                print(f"[DEBUG] Parsing unknown macro statement at pos {self.pos}, token: '{token_text}'")
                start_pos = self.pos
                unknown_stmt = self.parse_unknown_statement()
                if unknown_stmt:
                    macro_body.append(unknown_stmt)
                
                # Safety check - ensure we advanced
                if self.pos == start_pos:
                    print(f"[ERROR] Stuck in macro body at pos {self.pos}, forcing advance")
                    self.advance()
            
            # Check if position advanced in main macro loop
            if self.pos == old_pos:
                print(f"[ERROR] Macro parser stuck at position {self.pos}, forcing advance")
                self.advance()
        
        self.current_level -= 1  # Decrement level when exiting macro
        
        # Find %MEND; statement and validate nesting
        end_token = start_token
        if self.match_token_type(TokenType.KWM_MEND):
            # Validate that this %MEND matches our macro
            if self.pop_parsing_context("%MEND"):
                mend_token = self.current_token()
                self.advance()  # consume '%mend'
                
                # Consume macro name if present (like "inner2" in "%mend inner2;")
                if self.match_token_type(TokenType.MACRO_STRING) or self.match_token_type(TokenType.IDENTIFIER):
                    mend_macro_name = self.get_token_text(self.current_token())
                    print(f"[DEBUG] Found macro name '{mend_macro_name}' after %mend")
                    self.advance()  # consume macro name
                
                # Consume semicolon
                if self.match_token_type(TokenType.SEMI):
                    end_token = self.current_token()
                    self.advance()  # consume ';'
            else:
                print(f"[ERROR] Mismatched %MEND at pos {self.pos}")
                # Still consume the token to continue parsing
                self.advance()
        else:
            print(f"[ERROR] Expected %MEND for macro but didn't find it at pos {self.pos}")
            # Clean up stack
            self.pop_parsing_context("%MEND")
        
        macro_node = self.create_base_node(start_token, end_token)
        macro_node.update({
            'type': 'SasMacroDef',
            'macro_name': macro_name,
            'parameters': parameters,
            'macro_body': macro_body
        })
        
        return macro_node
    
    def parse_let_statement(self) -> Dict[str, Any]:
        """Parse %LET statement"""
        start_token = self.current_token()
        self.advance()  # consume '%let'
        
        # Get variable name - handle different token types
        var_name = ""
        if self.match_token_type(TokenType.IDENTIFIER) or self.match_token_type(TokenType.MACRO_STRING):
            var_name = self.get_token_text(self.current_token())
            self.advance()
        
        # Consume '='
        if self.match_token_type(TokenType.ASSIGN):
            self.advance()
        
        # Get value - handle different token types
        value = ""
        if (self.match_token_type(TokenType.IDENTIFIER) or 
            self.match_token_type(TokenType.STRING_LITERAL) or
            self.match_token_type(TokenType.MACRO_STRING)):
            value = self.get_token_text(self.current_token())
            self.advance()
        
        # Consume ';'
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        let_node = self.create_base_node(start_token, end_token)
        let_node.update({
            'type': 'SasLetStatement',
            'name': var_name,
            'value': value,
            'is_global': None
        })
        
        return let_node
    
    def parse_include_statement(self) -> Dict[str, Any]:
        """Parse %INCLUDE statement"""
        start_token = self.current_token()
        self.advance()  # consume '%include'
        
        # Get path
        path_token = self.current_token()
        path = self.get_token_text(path_token) if path_token else ""
        if self.match_token_type(TokenType.STRING_LITERAL):
            self.advance()
        
        # Consume ';'
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        include_node = self.create_base_node(start_token, end_token)
        include_node.update({
            'type': 'SasIncludeStatement',
            'path': path,
            'options': {}
        })
        
        return include_node
    
    def parse_macro_if_statement(self) -> Dict[str, Any]:
        """Parse %IF/%THEN/%ELSE statement"""
        start_token = self.current_token()
        self.advance()  # consume '%if'
        
        # Parse condition (collect until %THEN) with safety limit
        condition_tokens = []
        condition_iteration_count = 0
        max_condition_iterations = 100  # Safety limit for condition parsing
        
        while (not self.at_end() and 
               not self.match_token_type(TokenType.KWM_THEN) and
               condition_iteration_count < max_condition_iterations):
            condition_iteration_count += 1
            old_pos = self.pos
            
            current_token = self.current_token()
            token_text = self.get_token_text(current_token) if current_token else "None"
            
            if self.pos >= 2115:  # Near our problem area
                print(f"[DEBUG] Macro IF condition parsing token '{token_text}' at pos {self.pos}")
            
            condition_tokens.append(token_text)
            self.advance()
            
            # Safety check for infinite loops
            if self.pos == old_pos:
                print(f"[ERROR] Macro IF condition parser stuck at position {self.pos}, forcing break")
                break
                
        if condition_iteration_count >= max_condition_iterations:
            print(f"[ERROR] Macro IF condition parsing exceeded {max_condition_iterations} iterations, breaking")
            print(f"[DEBUG] Tokens collected so far: {' '.join(condition_tokens[:10])}...")
        
        condition = ' '.join(condition_tokens).strip()
        print(f"[DEBUG] Macro IF condition: '{condition[:50]}...', looking for %THEN at pos {self.pos}")
        
        # Parse %THEN block
        then_block = None
        if self.match_token_type(TokenType.KWM_THEN):
            print(f"[DEBUG] Found %THEN at pos {self.pos}, parsing THEN block")
            self.advance()  # consume '%then'
            then_block = self.parse_macro_statement_block()
        else:
            current_token = self.current_token()
            token_text = self.get_token_text(current_token) if current_token else "None"
            print(f"[DEBUG] No %THEN found at pos {self.pos}, current token: '{token_text}'")
        
        # Parse %ELSE block if present
        else_block = None
        if self.match_token_type(TokenType.KWM_ELSE):
            self.advance()  # consume '%else'
            else_block = self.parse_macro_statement_block()
        
        # Determine end token
        end_token = self.current_token() or start_token
        
        if_node = self.create_base_node(start_token, end_token)
        if_node.update({
            'type': 'SasMacroIfStatement',
            'condition': condition,
            'then_block': then_block,
            'else_block': else_block
        })
        
        return if_node
    
    def parse_macro_do_statement(self) -> Dict[str, Any]:
        """Parse %DO/%END block"""
        start_token = self.current_token()
        self.advance()  # consume '%do'
        
        # Consume semicolon after %DO if present
        if self.match_token_type(TokenType.SEMI):
            self.advance()
        
        # Parse statements until %END with safety limit
        statements = []
        do_iteration_count = 0
        max_do_iterations = 500  # Safety limit for %DO block parsing
        
        while (not self.at_end() and 
               not self.match_token_type(TokenType.KWM_END) and
               do_iteration_count < max_do_iterations):
            
            do_iteration_count += 1
            old_pos = self.pos
            current_token = self.current_token()
            
            if do_iteration_count % 10 == 0 or self.pos >= 2440:  # More frequent logging near problem area
                token_text = self.get_token_text(current_token) if current_token else "None"
                print(f"[DEBUG] Macro DO block iteration {do_iteration_count}, pos {self.pos}, token: '{token_text}'")
            
            if do_iteration_count > max_do_iterations:
                print(f"[ERROR] Macro DO block parsing exceeded {max_do_iterations} iterations, breaking")
                break
            
            print(f"[DEBUG] Calling parse_single_macro_statement from DO block at pos {self.pos}")
            stmt = self.parse_single_macro_statement()
            if stmt:
                statements.append(stmt)
            else:
                print(f"[DEBUG] parse_single_macro_statement returned None at pos {self.pos}")
            
            # Safety check for infinite loops
            if self.pos == old_pos:
                print(f"[ERROR] Macro DO block parser stuck at position {self.pos}, forcing advance")
                self.advance()
                
        if do_iteration_count >= max_do_iterations:
            print(f"[ERROR] Macro DO block parsing exceeded {max_do_iterations} iterations, breaking")
        
        # Consume %END
        end_token = start_token
        if self.match_token_type(TokenType.KWM_END):
            end_token = self.current_token()
            self.advance()
        
        # Consume semicolon after %END if present
        if self.match_token_type(TokenType.SEMI):
            self.advance()
        
        do_node = self.create_base_node(start_token, end_token)
        do_node.update({
            'type': 'SasMacroDoBlock',
            'statements': statements
        })
        
        return do_node
    
    def parse_macro_statement_block(self) -> Dict[str, Any]:
        """Parse a macro statement block (single statement or %DO block)"""
        current_token = self.current_token()
        if not current_token:
            return None
        
        token_text = self.get_token_text(current_token)
        print(f"[DEBUG] parse_macro_statement_block at pos {self.pos}, token: '{token_text}'")
        
        # Check if this is a %DO block
        if self.match_token_type(TokenType.KWM_DO):
            print(f"[DEBUG] Found %DO, calling parse_macro_do_statement")
            return self.parse_macro_do_statement()
        else:
            print(f"[DEBUG] Not %DO, calling parse_single_macro_statement")
            # Parse single statement
            return self.parse_single_macro_statement()
    
    def parse_single_macro_statement(self) -> Dict[str, Any]:
        """Parse a single macro statement"""
        if self.at_end():
            return None
        
        if self.match_token_type(TokenType.KWM_LET):
            return self.parse_let_statement()
        elif self.match_token_type(TokenType.KWM_INCLUDE):
            return self.parse_include_statement()
        elif self.match_token_type(TokenType.KW_LIBNAME):
            return self.parse_libname_statement()
        elif self.match_token_type(TokenType.KW_FILENAME):
            return self.parse_filename_statement()
        elif self.match_token_type(TokenType.KWM_IF):
            return self.parse_macro_if_statement()
        elif self.match_token_type(TokenType.KWM_DO):
            return self.parse_macro_do_statement()
        elif self.match_token_type(TokenType.KWM_MACRO):
            print(f"[DEBUG] Parsing nested MACRO in single macro statement at pos {self.pos}")
            return self.parse_macro_def()
        elif self.match_token_type(TokenType.PERCENT):
            print(f"[DEBUG] Parsing macro call in single macro statement at pos {self.pos}")
            # Check if this is a macro call
            macro_call = self.parse_macro_call()
            if macro_call:
                return macro_call
            else:
                return self.parse_unknown_statement()
        elif self.match_token_type(TokenType.KW_DATA):
            return self.parse_data_step()
        elif self.match_token_type(TokenType.KW_PROC):
            print(f"[DEBUG] parse_single_macro_statement calling parse_proc_step at pos {self.pos}")
            result = self.parse_proc_step()
            print(f"[DEBUG] parse_proc_step completed, returned to pos {self.pos}")
            return result
        elif self.match_token_type(TokenType.KW_CONNECT):
            print(f"[DEBUG] Parsing CONNECT TO statement in macro at pos {self.pos}")
            return self.parse_sql_connect_statement()
        elif self.match_token_type(TokenType.KW_EXECUTE):
            print(f"[DEBUG] Parsing EXECUTE BY statement in macro at pos {self.pos}")
            return self.parse_sql_execute_statement()
        elif self.match_token_type(TokenType.KW_DISCONNECT):
            print(f"[DEBUG] Parsing DISCONNECT FROM statement in macro at pos {self.pos}")
            return self.parse_sql_disconnect_statement()
        elif self.match_token_type(TokenType.KW_FORMAT):
            print(f"[DEBUG] Parsing FORMAT statement in macro at pos {self.pos}")
            return self.parse_format_statement()
        elif self.match_token_type(TokenType.KW_END):
            print(f"[DEBUG] Handling regular END token in single macro statement at pos {self.pos}")
            # Regular END statement - create simple statement and advance
            start_token = self.current_token()
            self.advance()  # consume 'END'
            
            # Consume semicolon if present
            end_token = start_token
            if self.match_token_type(TokenType.SEMI):
                end_token = self.current_token()
                self.advance()
            
            end_stmt = self.create_base_node(start_token, end_token)
            end_stmt.update({
                'type': 'SasEndStatement',
                'text': 'END'
            })
            return end_stmt
        elif self.match_token_type(TokenType.IDENTIFIER):
            current_token = self.current_token()
            token_text = self.get_token_text(current_token) if current_token else "None"
            print(f"[DEBUG] Checking IDENTIFIER '{token_text}' for assignment in macro at pos {self.pos}")
            
            # Check if this is an assignment statement by looking ahead for assignment operator
            # Pattern: IDENTIFIER + MACRO_VAR_RESOLVE + MACRO_STRING + ASSIGN (for NumSearches_&type=0;)
            lookahead_pos = 1
            found_assignment = False
            
            # Look ahead to find assignment operator, accounting for macro variables
            while lookahead_pos <= 10:  # Reasonable lookahead limit
                lookahead_token = self.peek_token(lookahead_pos)
                if not lookahead_token:
                    break
                
                lookahead_type = lookahead_token.token_type
                lookahead_text = self.get_token_text(lookahead_token)
                print(f"[DEBUG] Lookahead {lookahead_pos}: type={lookahead_type}, text='{lookahead_text}'")
                
                if lookahead_type == TokenType.ASSIGN:
                    found_assignment = True
                    print(f"[DEBUG] Found assignment operator at lookahead position {lookahead_pos}")
                    break
                elif lookahead_type == TokenType.SEMI:
                    print(f"[DEBUG] Found semicolon at lookahead position {lookahead_pos}, no assignment")
                    break  # End of statement without assignment
                elif lookahead_type in [TokenType.MACRO_VAR_RESOLVE, TokenType.MACRO_STRING, TokenType.WS]:
                    # These are expected parts of a macro variable, continue looking
                    print(f"[DEBUG] Skipping macro/whitespace token at lookahead {lookahead_pos}")
                    pass
                else:
                    # Some other token type that shouldn't be in a variable name
                    print(f"[DEBUG] Unexpected token type {lookahead_type} at lookahead {lookahead_pos}, stopping")
                    break
                lookahead_pos += 1
            
            if found_assignment:
                print(f"[DEBUG] Parsing assignment statement in macro at pos {self.pos}")
                return self.parse_macro_assignment_statement()
            else:
                # Check if this could be a bare macro call
                identifier_text = self.get_token_text(self.current_token())
                # Look ahead for semicolon
                if self.peek_token() and self.peek_token().token_type == TokenType.SEMI:
                    # Check if this identifier matches a known macro name
                    if self._is_known_macro_name(identifier_text):
                        print(f"[DEBUG] Parsing bare macro call '{identifier_text}' in macro")
                        macro_call = self.parse_bare_macro_call()
                        if macro_call:
                            return macro_call
                
                # Fall through to unknown statement if not a macro call
                print(f"[DEBUG] IDENTIFIER '{identifier_text}' not assignment or known macro, parsing as unknown")
                return self.parse_unknown_statement()
        else:
            # Unknown macro statement - skip until semicolon
            current_token = self.current_token()
            token_text = self.get_token_text(current_token) if current_token else "None"
            print(f"[DEBUG] parse_single_macro_statement unknown branch at pos {self.pos}, token: '{token_text}'")
            
            start_token = self.current_token()
            tokens_text = []
            unknown_iteration_count = 0
            max_unknown_iterations = 100  # Safety limit
            
            while (not self.at_end() and 
                   not self.match_token_type(TokenType.SEMI) and
                   not self.match_token_type(TokenType.KWM_END) and
                   unknown_iteration_count < max_unknown_iterations):
                unknown_iteration_count += 1
                old_pos = self.pos
                
                tokens_text.append(self.get_token_text(self.current_token()))
                self.advance()
                
                # Safety check for infinite loops
                if self.pos == old_pos:
                    print(f"[ERROR] parse_single_macro_statement unknown handler stuck at position {self.pos}, breaking")
                    break
                    
            if unknown_iteration_count >= max_unknown_iterations:
                print(f"[ERROR] parse_single_macro_statement unknown handler exceeded {max_unknown_iterations} iterations, breaking")
            
            # Include semicolon if present
            end_token = start_token
            if self.match_token_type(TokenType.SEMI):
                end_token = self.current_token()
                tokens_text.append(self.get_token_text(end_token))
                self.advance()
            
            unknown_node = self.create_base_node(start_token, end_token)
            unknown_node.update({
                'type': 'SasMacroUnknownStatement',
                'kind': 'unknown',
                'text': ''.join(tokens_text)
            })
            
            return unknown_node
    
    def parse_macro_call(self) -> Optional[Dict[str, Any]]:
        """Parse macro call like %importSheets(...)"""
        start_token = self.current_token()
        if not self.match_token_type(TokenType.PERCENT):
            return None
            
        self.advance()  # consume '%'
        
        # Get macro name
        macro_name = "unknown"
        if self.match_token_type(TokenType.IDENTIFIER):
            macro_name = self.get_token_text(self.current_token())
            self.advance()
        else:
            return None  # Not a valid macro call
        
        # Parse arguments if present
        args = []
        if self.match_token_type(TokenType.LPAREN):
            self.advance()  # consume '('
            
            # Collect arguments until ')'
            while not self.at_end() and not self.match_token_type(TokenType.RPAREN):
                token = self.current_token()
                if token:
                    args.append(self.get_token_text(token))
                self.advance()
            
            if self.match_token_type(TokenType.RPAREN):
                self.advance()  # consume ')'
        
        # Find end of statement
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        macro_call_node = self.create_base_node(start_token, end_token)
        macro_call_node.update({
            'type': 'SasMacroCall',
            'macro_name': macro_name,
            'arguments': {
                'positional': args,
                'keyword': {},
                'raw_order': args
            }
        })
        
        return macro_call_node
    
    def parse_macro_identifier_call(self) -> Optional[Dict[str, Any]]:
        """Parse macro call that starts with MACRO_IDENTIFIER token (like %importSheets)"""
        start_token = self.current_token()
        if not self.match_token_type(TokenType.MACRO_IDENTIFIER):
            return None
            
        # Get macro name from MACRO_IDENTIFIER token
        macro_name_token = self.current_token()
        macro_name_full = self.get_token_text(macro_name_token)
        
        # Remove % prefix if present
        macro_name = macro_name_full.lstrip('%')
        self.advance()  # consume MACRO_IDENTIFIER
        
        # Parse arguments if present
        args = []
        if self.match_token_type(TokenType.LPAREN):
            self.advance()  # consume '('
            
            # Collect arguments until ')'
            while not self.at_end() and not self.match_token_type(TokenType.RPAREN):
                token = self.current_token()
                if token:
                    args.append(self.get_token_text(token))
                self.advance()
            
            if self.match_token_type(TokenType.RPAREN):
                self.advance()  # consume ')'
        
        # Find end of statement
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        macro_call_node = self.create_base_node(start_token, end_token)
        macro_call_node.update({
            'type': 'SasMacroCall',
            'macro_name': macro_name,
            'arguments': {
                'positional': args,
                'keyword': {},
                'raw_order': args
            }
        })
        
        return macro_call_node
    
    def parse_libname_statement(self) -> Dict[str, Any]:
        """Parse LIBNAME statement with comprehensive option support"""
        start_token = self.current_token()
        self.advance()  # consume 'libname'
        
        # Get libref
        libref = "unknown"
        if self.match_token_type(TokenType.IDENTIFIER):
            libref = self.get_token_text(self.current_token())
            self.advance()
        
        # Initialize variables for different types of options
        path = ""
        engine = None
        options = {}
        
        # Parse all options until semicolon
        while not self.at_end() and not self.match_token_type(TokenType.SEMI):
            if self.match_token_type(TokenType.IDENTIFIER):
                option_name = self.get_token_text(self.current_token()).upper()
                self.advance()
                
                # Handle different option types
                if option_name in ['META', 'BASE', 'V8', 'V9', 'XPORT', 'ORACLE', 'OLEDB', 'ODBC']:
                    # Engine specification
                    engine = option_name
                    
                elif option_name in ['LIBRARY', 'REPNAME', 'LIBURI', 'SERVER', 'USER', 'PASSWORD', 'SCHEMA']:
                    # Options with values (expect = and value)
                    if self.match_token_type(TokenType.ASSIGN):
                        self.advance()  # consume '='
                        
                        # Get option value (can be string literal, identifier, or complex)
                        option_value = ""
                        if self.match_token_type(TokenType.STRING_LITERAL):
                            option_value = self.get_token_text(self.current_token())
                            self.advance()
                        elif self.match_token_type(TokenType.IDENTIFIER):
                            option_value = self.get_token_text(self.current_token())
                            self.advance()
                        else:
                            # Handle complex values like SAS URIs - collect until space or semicolon
                            complex_tokens = []
                            paren_count = 0
                            bracket_count = 0
                            
                            while (not self.at_end() and 
                                   not (self.match_token_type(TokenType.SEMI) and paren_count == 0 and bracket_count == 0) and
                                   not (self.match_token_type(TokenType.IDENTIFIER) and paren_count == 0 and bracket_count == 0)):
                                
                                current_text = self.get_token_text(self.current_token())
                                
                                # Track parentheses and brackets for complex URIs
                                if current_text == '(':
                                    paren_count += 1
                                elif current_text == ')':
                                    paren_count -= 1
                                elif current_text == '[':
                                    bracket_count += 1
                                elif current_text == ']':
                                    bracket_count -= 1
                                
                                complex_tokens.append(current_text)
                                self.advance()
                                
                                # Break if we've closed all brackets/parens and hit a space
                                if paren_count == 0 and bracket_count == 0:
                                    next_token = self.current_token()
                                    if (next_token and 
                                        (self.match_token_type(TokenType.IDENTIFIER) or 
                                         self.match_token_type(TokenType.SEMI))):
                                        break
                            
                            option_value = ''.join(complex_tokens)
                        
                        options[option_name.lower()] = option_value
                    else:
                        # Option without explicit value (like META)
                        if option_name == 'META':
                            engine = 'META'
                        else:
                            options[option_name.lower()] = True
                            
                elif option_name.startswith("'") or option_name.startswith('"'):
                    # This is actually a path/string literal misidentified as IDENTIFIER
                    path = option_name
                    
                else:
                    # Unknown option - store as-is
                    options[option_name.lower()] = True
                    
            elif self.match_token_type(TokenType.STRING_LITERAL):
                # Direct string literal - could be path
                if not path:  # Only set path if not already set
                    path = self.get_token_text(self.current_token())
                else:
                    # Additional string - add to options
                    options['additional_string'] = self.get_token_text(self.current_token())
                self.advance()
                
            else:
                # Skip other token types (operators, etc.)
                self.advance()
        
        # Consume semicolon
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        libname_node = self.create_base_node(start_token, end_token)
        libname_node.update({
            'type': 'SasLibnameStatement',
            'libref': libref,
            'path': path,
            'engine': engine,
            'options': options
        })
        
        return libname_node
    
    def parse_filename_statement(self) -> Dict[str, Any]:
        """Parse FILENAME statement"""
        start_token = self.current_token()
        self.advance()  # consume 'filename'
        
        # Get fileref
        fileref = "unknown"
        if self.match_token_type(TokenType.IDENTIFIER):
            fileref = self.get_token_text(self.current_token())
            self.advance()
        
        # Get path or device
        path = ""
        device = None
        if self.match_token_type(TokenType.STRING_LITERAL):
            path = self.get_token_text(self.current_token())
            self.advance()
        elif self.match_token_type(TokenType.IDENTIFIER):
            device = self.get_token_text(self.current_token())
            self.advance()
        
        # Parse additional options until semicolon
        options = {}
        while not self.at_end() and not self.match_token_type(TokenType.SEMI):
            self.advance()  # Skip for now
        
        # Consume semicolon
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        filename_node = self.create_base_node(start_token, end_token)
        filename_node.update({
            'type': 'SasFilenameStatement',
            'fileref': fileref,
            'path': path,
            'device': device,
            'options': options
        })
        
        return filename_node
    
    def parse_set_statement(self) -> Dict[str, Any]:
        """Parse SET statement"""
        start_token = self.current_token()
        self.advance()  # consume 'set'
        
        # Parse dataset references
        datasets = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI)):
            if self.match_token_type(TokenType.IDENTIFIER):
                dataset_name = self.get_token_text(self.current_token())
                self.advance()
                
                # Check for libref.dataset pattern
                if self.match_token_type(TokenType.DOT):
                    self.advance()  # consume '.'
                    if self.match_token_type(TokenType.IDENTIFIER):
                        table_name = self.get_token_text(self.current_token())
                        dataset_name = f"{dataset_name}.{table_name}"
                        self.advance()
                
                # Create dataset reference
                dataset_ref = {
                    'type': 'SasDatasetRef',
                    'name': dataset_name,
                    'options': {}
                }
                datasets.append(dataset_ref)
            else:
                self.advance()  # skip other tokens
        
        # Consume semicolon
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        set_node = self.create_base_node(start_token, end_token)
        set_node.update({
            'type': 'SasSetStatement',
            'datasets': datasets
        })
        
        return set_node
    
    def parse_merge_statement(self) -> Dict[str, Any]:
        """Parse MERGE statement"""
        start_token = self.current_token()
        self.advance()  # consume 'merge'
        
        # Parse dataset references
        datasets = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI) and
               not self.match_token_type(TokenType.KW_BY)):
            if self.match_token_type(TokenType.IDENTIFIER):
                dataset_name = self.get_token_text(self.current_token())
                self.advance()
                
                # Handle libref.dataset pattern
                if self.match_token_type(TokenType.DOT):
                    self.advance()
                    if self.match_token_type(TokenType.IDENTIFIER):
                        table_name = self.get_token_text(self.current_token())
                        dataset_name = f"{dataset_name}.{table_name}"
                        self.advance()
                
                datasets.append({
                    'type': 'SasDatasetRef',
                    'name': dataset_name,
                    'options': {}
                })
            else:
                self.advance()
        
        # Parse BY variables if present
        by_variables = []
        if self.match_token_type(TokenType.KW_BY):
            self.advance()  # consume 'by'
            while (not self.at_end() and 
                   not self.match_token_type(TokenType.SEMI)):
                if self.match_token_type(TokenType.IDENTIFIER):
                    by_variables.append(self.get_token_text(self.current_token()))
                    self.advance()
                else:
                    self.advance()
        
        # Consume semicolon
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        merge_node = self.create_base_node(start_token, end_token)
        merge_node.update({
            'type': 'SasMergeStatement',
            'datasets': datasets,
            'by_variables': by_variables
        })
        
        return merge_node
    
    def parse_update_statement(self) -> Dict[str, Any]:
        """Parse UPDATE statement"""
        start_token = self.current_token()
        self.advance()  # consume 'update'
        
        # Parse master dataset
        master_dataset = "unknown"
        if self.match_token_type(TokenType.IDENTIFIER):
            master_dataset = self.get_token_text(self.current_token())
            self.advance()
            
            # Handle libref.dataset pattern
            if self.match_token_type(TokenType.DOT):
                self.advance()
                if self.match_token_type(TokenType.IDENTIFIER):
                    table_name = self.get_token_text(self.current_token())
                    master_dataset = f"{master_dataset}.{table_name}"
                    self.advance()
        
        # Parse transaction dataset
        transaction_dataset = "unknown"
        if self.match_token_type(TokenType.IDENTIFIER):
            transaction_dataset = self.get_token_text(self.current_token())
            self.advance()
            
            # Handle libref.dataset pattern
            if self.match_token_type(TokenType.DOT):
                self.advance()
                if self.match_token_type(TokenType.IDENTIFIER):
                    table_name = self.get_token_text(self.current_token())
                    transaction_dataset = f"{transaction_dataset}.{table_name}"
                    self.advance()
        
        # Parse BY variables if present
        by_variables = []
        if self.match_token_type(TokenType.KW_BY):
            self.advance()  # consume 'by'
            while (not self.at_end() and 
                   not self.match_token_type(TokenType.SEMI)):
                if self.match_token_type(TokenType.IDENTIFIER):
                    by_variables.append(self.get_token_text(self.current_token()))
                    self.advance()
                else:
                    self.advance()
        
        # Consume semicolon
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        update_node = self.create_base_node(start_token, end_token)
        update_node.update({
            'type': 'SasUpdateStatement',
            'master_dataset': master_dataset,
            'transaction_dataset': transaction_dataset,
            'by_variables': by_variables
        })
        
        return update_node
    
    def parse_if_statement(self) -> Dict[str, Any]:
        """Parse IF/THEN/ELSE statement with proper multi-line and DO block support"""
        start_token = self.current_token()
        self.advance()  # consume 'if'
        
        # Parse condition (collect until THEN)
        condition_tokens = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.KW_THEN)):
            condition_tokens.append(self.get_token_text(self.current_token()))
            self.advance()
        
        condition = ' '.join(condition_tokens).strip()
        
        # Parse THEN clause
        then_block = None
        if self.match_token_type(TokenType.KW_THEN):
            self.advance()  # consume 'then'
            then_block = self.parse_statement_block()
        
        # Consume semicolon after THEN block if present
        if self.match_token_type(TokenType.SEMI):
            self.advance()
        
        # Look ahead for ELSE clause (might be on next line)
        else_block = None
        if self.match_token_type(TokenType.KW_ELSE):
            self.advance()  # consume 'else'
            else_block = self.parse_statement_block()
            
            # Consume semicolon after ELSE block if present
            if self.match_token_type(TokenType.SEMI):
                self.advance()
        
        # Determine end token
        end_token = self.current_token() or start_token
        
        if_node = self.create_base_node(start_token, end_token)
        if_node.update({
            'type': 'SasIfStatement',
            'condition': condition,
            'then_block': then_block,
            'else_block': else_block
        })
        
        return if_node
    
    def parse_statement_block(self) -> Dict[str, Any]:
        """Parse a statement block (single statement or DO...END block)"""
        if not self.current_token():
            return None
        
        start_token = self.current_token()
        
        # Check if this is a DO block
        if self.match_token_type(TokenType.KW_DO):
            return self.parse_do_block()
        else:
            # Parse single statement
            return self.parse_single_statement_in_block()
    
    def parse_do_block(self) -> Dict[str, Any]:
        """Parse DO...END block"""
        start_token = self.current_token()
        self.advance()  # consume 'do'
        
        # Consume semicolon after DO if present
        if self.match_token_type(TokenType.SEMI):
            self.advance()
        
        # Parse statements until END
        statements = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.KW_END)):
            
            stmt = self.parse_single_data_step_statement()
            if stmt:
                statements.append(stmt)
        
        # Consume END
        end_token = start_token
        if self.match_token_type(TokenType.KW_END):
            end_token = self.current_token()
            self.advance()
        
        # Consume semicolon after END if present
        if self.match_token_type(TokenType.SEMI):
            self.advance()
        
        do_node = self.create_base_node(start_token, end_token)
        do_node.update({
            'type': 'SasDoBlock',
            'statements': statements
        })
        
        return do_node
    
    def parse_single_statement_in_block(self) -> Dict[str, Any]:
        """Parse a single statement within a THEN/ELSE block"""
        return self.parse_single_data_step_statement()
    
    def parse_single_data_step_statement(self) -> Dict[str, Any]:
        """Parse a single DATA step statement"""
        if self.at_end():
            return None
            
        if self.match_token_type(TokenType.KW_SET):
            return self.parse_set_statement()
        elif self.match_token_type(TokenType.KW_MERGE):
            return self.parse_merge_statement()
        elif self.match_token_type(TokenType.KW_UPDATE):
            return self.parse_update_statement()
        elif self.match_token_type(TokenType.KW_IF):
            return self.parse_if_statement()
        elif self.match_token_type(TokenType.KW_OUTPUT):
            return self.parse_output_statement()
        elif self.match_token_type(TokenType.PERCENT):
            print(f"[DEBUG] Parsing macro call in DATA step DO block at pos {self.pos}")
            # Handle macro calls in DATA step
            macro_call = self.parse_macro_call()
            if macro_call:
                return macro_call
            else:
                return self.parse_unknown_statement()
        elif self.match_token_type(TokenType.MACRO_IDENTIFIER):
            print(f"[DEBUG] Parsing MACRO_IDENTIFIER as macro call in DATA step DO block at pos {self.pos}")
            macro_call = self.parse_macro_identifier_call()
            if macro_call:
                return macro_call
            else:
                return self.parse_unknown_statement()
        elif self.match_token_type(TokenType.KW_END):
            print(f"[DEBUG] Handling regular END token in data step at pos {self.pos}")
            # Regular END statement - create simple statement and advance
            start_token = self.current_token()
            self.advance()  # consume 'END'
            
            # Consume semicolon if present
            end_token = start_token
            if self.match_token_type(TokenType.SEMI):
                end_token = self.current_token()
                self.advance()
            
            end_stmt = self.create_base_node(start_token, end_token)
            end_stmt.update({
                'type': 'SasEndStatement',
                'text': 'END'
            })
            return end_stmt
        elif self.match_token_type(TokenType.IDENTIFIER):
            # Check if this is an assignment statement
            next_token = self.peek_token()
            if next_token and next_token.token_type == TokenType.ASSIGN:
                return self.parse_assignment_statement()
            else:
                # Check if this could be a bare macro call
                identifier_text = self.get_token_text(self.current_token())
                # Look ahead for semicolon
                if self.peek_token() and self.peek_token().token_type == TokenType.SEMI:
                    # Check if this identifier matches a known macro name
                    if self._is_known_macro_name(identifier_text):
                        print(f"[DEBUG] Parsing bare macro call '{identifier_text}' in DATA step DO block")
                        macro_call = self.parse_bare_macro_call()
                        if macro_call:
                            return macro_call
                
                # Fall through to unknown statement if not a macro call
                return self.parse_unknown_statement()
        else:
            # Unknown statement - skip until semicolon
            return self.parse_unknown_statement()
    
    def parse_assignment_statement(self) -> Dict[str, Any]:
        """Parse assignment statement (var = expression)"""
        start_token = self.current_token()
        
        # Parse variable name
        variable_name = self.get_token_text(self.current_token())
        self.advance()
        
        # Consume assignment operator
        if self.match_token_type(TokenType.ASSIGN):
            self.advance()
        
        # Parse expression (simplified - collect until semicolon)
        expression_tokens = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI)):
            expression_tokens.append(self.get_token_text(self.current_token()))
            self.advance()
        
        expression = ' '.join(expression_tokens).strip()
        
        # Consume semicolon
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        assignment_node = self.create_base_node(start_token, end_token)
        assignment_node.update({
            'type': 'SasAssignmentStatement',
            'variable': variable_name,
            'expression': expression
        })
        
        return assignment_node
    
    def parse_output_statement(self) -> Dict[str, Any]:
        """Parse OUTPUT statement"""
        start_token = self.current_token()
        self.advance()  # consume 'output'
        
        # Parse optional dataset names
        datasets = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI)):
            if self.match_token_type(TokenType.IDENTIFIER):
                dataset_name = self.get_token_text(self.current_token())
                self.advance()
                
                # Handle libref.dataset pattern
                if self.match_token_type(TokenType.DOT):
                    self.advance()
                    if self.match_token_type(TokenType.IDENTIFIER):
                        table_name = self.get_token_text(self.current_token())
                        dataset_name = f"{dataset_name}.{table_name}"
                        self.advance()
                
                datasets.append(dataset_name)
            else:
                self.advance()
        
        # Consume semicolon
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        output_node = self.create_base_node(start_token, end_token)
        output_node.update({
            'type': 'SasOutputStatement',
            'datasets': datasets if datasets else None
        })
        
        return output_node
    
    def parse_proc_sql_statement(self) -> Optional[Dict[str, Any]]:
        """Parse SQL statement within PROC SQL"""
        start_token = self.current_token()
        if not start_token:
            return None
        
        # Determine SQL statement type
        if self.match_token_type(TokenType.KW_CREATE):
            return self.parse_sql_create_statement()
        elif self.match_token_type(TokenType.KW_SELECT):
            return self.parse_sql_select_statement()
        elif self.match_token_type(TokenType.KW_INSERT):
            return self.parse_sql_insert_statement()
        elif self.match_token_type(TokenType.KW_UPDATE):
            return self.parse_sql_update_statement()
        elif self.match_token_type(TokenType.KW_DELETE):
            return self.parse_sql_delete_statement()
        elif self.match_token_type(TokenType.KW_DROP):
            return self.parse_sql_drop_statement()
        elif self.match_token_type(TokenType.KW_CONNECT):
            return self.parse_sql_connect_statement()
        elif self.match_token_type(TokenType.KW_EXECUTE):
            return self.parse_sql_execute_statement()
        elif self.match_token_type(TokenType.KW_DISCONNECT):
            return self.parse_sql_disconnect_statement()
        elif self.match_token_type(TokenType.PERCENT):
            print(f"[DEBUG] Parsing macro call in PROC SQL at pos {self.pos}")
            return self.parse_macro_call()
        elif self.match_token_type(TokenType.MACRO_IDENTIFIER):
            print(f"[DEBUG] Parsing MACRO_IDENTIFIER as macro call in PROC SQL at pos {self.pos}")
            return self.parse_macro_identifier_call()
        else:
            return None
    
    def parse_sql_create_statement(self) -> Dict[str, Any]:
        """Parse CREATE TABLE/VIEW statement"""
        start_token = self.current_token()
        self.advance()  # consume 'create'
        
        # Check for TABLE or VIEW
        statement_type = "table"
        if self.match_token_type(TokenType.KW_TABLE):
            self.advance()
            statement_type = "table"
        elif self.match_token_type(TokenType.KW_VIEW):
            self.advance()
            statement_type = "view"
        
        # Get table/view name
        table_name = "unknown"
        if self.match_token_type(TokenType.IDENTIFIER):
            table_name = self.get_token_text(self.current_token())
            self.advance()
            # Handle libref.table pattern
            if self.match_token_type(TokenType.DOT):
                self.advance()
                if self.match_token_type(TokenType.IDENTIFIER):
                    table_name += "." + self.get_token_text(self.current_token())
                    self.advance()
        
        # Parse table options if present (like label="...")
        table_options = {}
        if self.match_token_type(TokenType.LPAREN):
            print(f"[DEBUG] Found table options at pos {self.pos}")
            self.advance()  # consume '('
            
            # Skip everything inside parentheses for now
            paren_depth = 1
            options_tokens = []
            while not self.at_end() and paren_depth > 0:
                token = self.current_token()
                token_text = self.get_token_text(token)
                
                if token_text == '(':
                    paren_depth += 1
                elif token_text == ')':
                    paren_depth -= 1
                
                if paren_depth > 0:  # Don't include the closing paren
                    options_tokens.append(token_text)
                
                self.advance()
            
            # Store the options text for now
            if options_tokens:
                table_options['raw'] = ''.join(options_tokens)
                print(f"[DEBUG] Table options: {table_options['raw'][:50]}...")
        
        # Parse AS SELECT if present
        select_statement = None
        if self.match_token_type(TokenType.KW_AS):
            self.advance()  # consume 'as'
            if self.match_token_type(TokenType.KW_SELECT):
                select_statement = self.parse_sql_select_statement()
        elif self.match_token_type(TokenType.KW_SELECT):
            # Direct SELECT without AS keyword
            select_statement = self.parse_sql_select_statement()
        else:
            # Skip to semicolon if no SELECT
            while (not self.at_end() and 
                   not self.match_token_type(TokenType.SEMI)):
                self.advance()
        
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        if statement_type == "table":
            create_node = self.create_base_node(start_token, end_token)
            create_node.update({
                'type': 'SasProcSqlCreateTableStatement',
                'table_name': table_name,
                'table_options': table_options if table_options else None,
                'select_statement': select_statement
            })
        else:
            create_node = self.create_base_node(start_token, end_token)
            create_node.update({
                'type': 'SasProcSqlCreateViewStatement',
                'view_name': table_name,
                'table_options': table_options if table_options else None,
                'select_statement': select_statement
            })
        
        return create_node
    
    def parse_sql_select_statement(self) -> Dict[str, Any]:
        """Parse SELECT statement with full SQL support including DISTINCT, INTO, JOINs"""
        start_token = self.current_token()
        self.advance()  # consume 'select'
        
        # Check for DISTINCT
        distinct = False
        if self.match_token_type(TokenType.KW_DISTINCT):
            distinct = True
            self.advance()  # consume 'distinct'
        
        # Parse columns - enhanced version
        columns = self.parse_sql_columns()
        
        # Parse INTO clause (SAS specific)
        into_clause = None
        if self.match_token_type(TokenType.KW_INTO):
            into_clause = self.parse_sql_into_clause()
        
        # Parse FROM clause with JOINs
        from_clause = None
        if self.match_token_type(TokenType.KW_FROM):
            from_clause = self.parse_sql_from_clause_with_joins()
        
        # Parse WHERE, GROUP BY, HAVING, ORDER BY
        where_clause = None
        if self.match_token_type(TokenType.KW_WHERE):
            where_clause = self.parse_where_clause()
        
        group_by_clause = None
        if self.match_token_type(TokenType.KW_GROUP):
            group_by_clause = self.parse_group_by_clause()
        
        having_clause = None  
        if self.match_token_type(TokenType.KW_HAVING):
            having_clause = self.parse_having_clause()
        
        order_by_clause = None
        if self.match_token_type(TokenType.KW_ORDER):
            order_by_clause = self.parse_order_by_clause()
        
        # Consume semicolon
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        select_node = self.create_base_node(start_token, end_token)
        select_node.update({
            'type': 'SasProcSqlSelectStatement',
            'distinct': distinct,
            'columns': columns,
            'into': into_clause,
            'from': from_clause,
            'where': where_clause,
            'group_by': group_by_clause,
            'having': having_clause,
            'order_by': order_by_clause
        })
        
        return select_node
    
    def parse_select_columns(self) -> List[Dict[str, Any]]:
        """Parse SELECT column list"""
        columns = []
        
        column_iteration_count = 0
        max_column_iterations = 500  # Safety limit for column parsing
        
        while not self.at_end() and column_iteration_count < max_column_iterations:
            column_iteration_count += 1
            old_pos = self.pos
            
            if (self.match_token_type(TokenType.KW_FROM) or 
                self.match_token_type(TokenType.KW_WHERE) or
                self.match_token_type(TokenType.KW_GROUP) or
                self.match_token_type(TokenType.KW_ORDER) or
                self.match_token_type(TokenType.SEMI)):
                break
                
            # Parse a single column expression
            column_expr = self.parse_column_expression()
            if column_expr:
                columns.append(column_expr)
            
            # Handle comma separation
            if self.current_token() and self.get_token_text(self.current_token()) == ',':
                self.advance()
            elif not (self.match_token_type(TokenType.KW_FROM) or 
                     self.match_token_type(TokenType.KW_WHERE) or
                     self.match_token_type(TokenType.KW_GROUP) or
                     self.match_token_type(TokenType.KW_ORDER) or
                     self.match_token_type(TokenType.SEMI)):
                # If no comma and not at a clause boundary, something went wrong
                print(f"[DEBUG] Column parsing break at pos {self.pos}")
                break
            
            # Safety check for infinite loops
            if self.pos == old_pos:
                print(f"[ERROR] Column parser stuck at position {self.pos}, forcing break")
                break
                
        if column_iteration_count >= max_column_iterations:
            print(f"[ERROR] Column parsing exceeded {max_column_iterations} iterations, breaking")
        
        return columns
    
    def parse_column_expression(self) -> Dict[str, Any]:
        """Parse a single column expression (simple column, function call, etc.)"""
        if not self.current_token():
            return None
            
        # Look ahead to see if this is a function call
        current_text = self.get_token_text(self.current_token())
        next_token = self.peek_token()
        
        if (self.match_token_type(TokenType.IDENTIFIER) and 
            next_token and self.get_token_text(next_token) == '('):
            # This is a function call like avg(salary) or count(*)
            return self.parse_function_call()
        elif self.match_token_type(TokenType.IDENTIFIER):
            # This is a simple column reference
            return self.parse_simple_column()
        else:
            # Skip unknown tokens
            self.advance()
            return None
    
    def parse_function_call(self) -> Dict[str, Any]:
        """Parse function call like avg(salary) as avg_salary"""
        func_name = self.get_token_text(self.current_token())
        self.advance()  # consume function name
        
        if not (self.current_token() and self.get_token_text(self.current_token()) == '('):
            # Not actually a function call
            return {'name': func_name, 'alias': None}
        
        self.advance()  # consume '('
        
        # Parse function parameters
        params = []
        param_text = ""
        paren_count = 1
        
        while self.current_token() and paren_count > 0:
            token_text = self.get_token_text(self.current_token())
            
            if token_text == '(':
                paren_count += 1
                param_text += token_text
            elif token_text == ')':
                paren_count -= 1
                if paren_count > 0:
                    param_text += token_text
            elif token_text == ',' and paren_count == 1:
                # Parameter separator at top level
                if param_text.strip():
                    params.append(param_text.strip())
                param_text = ""
            else:
                param_text += token_text
            
            self.advance()
        
        # Add the last parameter if any
        if param_text.strip():
            params.append(param_text.strip())
        
        # Build the function expression
        func_expr = f"{func_name}({', '.join(params)})"
        
        # Check for alias
        alias = None
        if self.match_token_type(TokenType.KW_AS):
            self.advance()  # consume 'as'
            if self.match_token_type(TokenType.IDENTIFIER):
                alias = self.get_token_text(self.current_token())
                self.advance()
        elif (self.match_token_type(TokenType.IDENTIFIER) and
              not self.get_token_text(self.current_token()) == ','):
            # Alias without AS keyword
            peek = self.peek_token()
            if not peek or self.get_token_text(peek) in [',', 'from', 'where', 'group', 'order', ';']:
                alias = self.get_token_text(self.current_token())
                self.advance()
        
        return {
            'name': func_expr,
            'alias': alias
        }
    
    def parse_simple_column(self) -> Dict[str, Any]:
        """Parse simple column reference like department or table.column"""
        column_name = self.get_token_text(self.current_token())
        self.advance()
        
        # Handle qualified names like table.column
        if self.match_token_type(TokenType.DOT):
            self.advance()  # consume '.'
            if self.match_token_type(TokenType.IDENTIFIER):
                column_name += "." + self.get_token_text(self.current_token())
                self.advance()
        
        # Check for alias
        alias = None
        if self.match_token_type(TokenType.KW_AS):
            self.advance()  # consume 'as'
            if self.match_token_type(TokenType.IDENTIFIER):
                alias = self.get_token_text(self.current_token())
                self.advance()
        elif (self.match_token_type(TokenType.IDENTIFIER) and
              not self.get_token_text(self.current_token()) == ','):
            # Alias without AS keyword - be careful not to consume next column
            peek = self.peek_token()
            if not peek or self.get_token_text(peek) in [',', 'from', 'where', 'group', 'order', ';']:
                alias = self.get_token_text(self.current_token())
                self.advance()
        
        return {
            'name': column_name,
            'alias': alias
        }
    
    def parse_from_clause(self) -> Dict[str, Any]:
        """Parse FROM clause"""
        start_token = self.current_token()
        self.advance()  # consume 'from'
        
        # Parse table name
        table_name = "unknown"
        if self.match_token_type(TokenType.IDENTIFIER):
            table_name = self.get_token_text(self.current_token())
            self.advance()
            
            # Handle libref.table pattern
            if self.match_token_type(TokenType.DOT):
                self.advance()
                if self.match_token_type(TokenType.IDENTIFIER):
                    table_name += "." + self.get_token_text(self.current_token())
                    self.advance()
        
        from_node = self.create_base_node(start_token, self.current_token() or start_token)
        from_node.update({
            'type': 'SasProcSqlFromClause',
            'table_name': table_name
        })
        
        return from_node
    
    def parse_where_clause(self) -> Dict[str, Any]:
        """Parse WHERE clause"""
        start_token = self.current_token()
        self.advance()  # consume 'where'
        
        # For now, just collect the condition text until next major clause
        condition_tokens = []
        while not self.at_end():
            if (self.match_token_type(TokenType.KW_GROUP) or
                self.match_token_type(TokenType.KW_ORDER) or
                self.match_token_type(TokenType.SEMI)):
                break
            
            condition_tokens.append(self.get_token_text(self.current_token()))
            self.advance()
        
        condition_text = " ".join(condition_tokens).strip()
        
        where_node = self.create_base_node(start_token, self.current_token() or start_token)
        where_node.update({
            'type': 'SasProcSqlWhereClause',
            'condition': condition_text
        })
        
        return where_node
    
    def parse_group_by_clause(self) -> Dict[str, Any]:
        """Parse GROUP BY clause"""
        start_token = self.current_token()
        self.advance()  # consume 'GROUP'
        self.advance()  # consume 'BY'
        
        # Parse column list
        columns = []
        while not self.at_end():
            if (self.match_token_type(TokenType.KW_ORDER) or 
                self.match_token_type(TokenType.SEMI) or
                self.match_token_type(TokenType.KW_HAVING)):
                break
                
            if self.match_token_type(TokenType.IDENTIFIER):
                column_name = self.get_token_text(self.current_token())
                columns.append({'name': column_name})
                self.advance()
                
                # Handle comma separation
                if self.current_token() and self.get_token_text(self.current_token()) == ',':
                    self.advance()
            else:
                self.advance()
        
        group_by_node = self.create_base_node(start_token, self.current_token() or start_token)
        group_by_node.update({
            'type': 'SasProcSqlGroupByClause',
            'columns': columns
        })
        
        return group_by_node
    
    def parse_having_clause(self) -> Dict[str, Any]:
        """Parse HAVING clause"""
        start_token = self.current_token()
        self.advance()  # consume 'having'
        
        # Collect HAVING condition tokens
        condition_tokens = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.KW_ORDER) and
               not self.match_token_type(TokenType.SEMI)):
            
            token = self.current_token()
            if token:
                condition_tokens.append(self.get_token_text(token))
            self.advance()
        
        condition = ' '.join(condition_tokens).strip()
        
        having_node = self.create_base_node(start_token, start_token)
        having_node.update({
            'type': 'SasProcSqlHavingClause',
            'condition': condition
        })
        
        return having_node
    
    def parse_order_by_clause(self) -> Dict[str, Any]:
        """Parse ORDER BY clause"""
        start_token = self.current_token()
        self.advance()  # consume 'ORDER'
        self.advance()  # consume 'BY'
        
        # Parse column list with optional ASC/DESC
        columns = []
        while not self.at_end():
            if self.match_token_type(TokenType.SEMI):
                break
                
            if self.match_token_type(TokenType.IDENTIFIER):
                column_name = self.get_token_text(self.current_token())
                self.advance()
                
                # Check for ASC/DESC (though we may not have these tokens)
                direction = 'ASC'  # default
                
                columns.append({
                    'name': column_name,
                    'direction': direction
                })
                
                # Handle comma separation
                if self.current_token() and self.get_token_text(self.current_token()) == ',':
                    self.advance()
            else:
                self.advance()
        
        order_by_node = self.create_base_node(start_token, self.current_token() or start_token)
        order_by_node.update({
            'type': 'SasProcSqlOrderByClause',
            'columns': columns
        })
        
        return order_by_node
    
    def parse_sql_insert_statement(self) -> Dict[str, Any]:
        """Parse INSERT statement"""
        start_token = self.current_token()
        
        # Skip to semicolon
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI)):
            self.advance()
        
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        insert_node = self.create_base_node(start_token, end_token)
        insert_node.update({
            'type': 'SasProcSqlInsertIntoStatement',
            'table_name': 'unknown',
            'values': []
        })
        
        return insert_node
    
    def parse_sql_update_statement(self) -> Dict[str, Any]:
        """Parse UPDATE statement"""
        start_token = self.current_token()
        
        # Skip to semicolon
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI)):
            self.advance()
        
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        update_node = self.create_base_node(start_token, end_token)
        update_node.update({
            'type': 'SasProcSqlUpdateStatement',
            'table_name': 'unknown',
            'set_clauses': {},
            'where_condition': ''
        })
        
        return update_node
    
    def parse_sql_delete_statement(self) -> Dict[str, Any]:
        """Parse DELETE statement"""
        start_token = self.current_token()
        
        # Skip to semicolon
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI)):
            self.advance()
        
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        delete_node = self.create_base_node(start_token, end_token)
        delete_node.update({
            'type': 'SasProcSqlDeleteFromStatement',
            'table_name': 'unknown',
            'where_condition': ''
        })
        
        return delete_node
    
    def parse_sql_drop_statement(self) -> Dict[str, Any]:
        """Parse DROP statement"""
        start_token = self.current_token()
        
        # Skip to semicolon
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI)):
            self.advance()
        
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        drop_node = self.create_base_node(start_token, end_token)
        drop_node.update({
            'type': 'SasProcSqlDropTableStatement',
            'table_name': 'unknown'
        })
        
        return drop_node
    
    def parse_sql_connect_statement(self) -> Dict[str, Any]:
        """Parse CONNECT TO statement"""
        start_token = self.current_token()
        self.advance()  # consume 'CONNECT'
        
        # Expect 'TO'
        provider = "unknown"
        if self.match_token_type(TokenType.KW_TO):
            self.advance()  # consume 'TO'
            
            # Get provider name
            if self.match_token_type(TokenType.IDENTIFIER):
                provider = self.get_token_text(self.current_token())
                self.advance()
        
        # Parse connection parameters (collect everything until semicolon)
        connection_params = []
        paren_depth = 0
        
        while not self.at_end() and not (self.match_token_type(TokenType.SEMI) and paren_depth == 0):
            token = self.current_token()
            token_text = self.get_token_text(token)
            
            if token_text == '(':
                paren_depth += 1
            elif token_text == ')':
                paren_depth -= 1
                
            connection_params.append(token_text)
            self.advance()
        
        # Consume semicolon
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        connect_node = self.create_base_node(start_token, end_token)
        connect_node.update({
            'type': 'SasProcSqlConnectStatement',
            'provider': provider,
            'connection_params': ''.join(connection_params).strip()
        })
        
        return connect_node
    
    def parse_sql_execute_statement(self) -> Dict[str, Any]:
        """Parse EXECUTE BY statement"""
        start_token = self.current_token()
        self.advance()  # consume 'EXECUTE'
        
        # Expect 'BY' - but it might be parsed as KW_BY or IDENTIFIER
        provider = "unknown"
        if (self.match_token_type(TokenType.KW_BY) or 
            (self.match_token_type(TokenType.IDENTIFIER) and 
             self.get_token_text(self.current_token()).upper() == "BY")):
            self.advance()  # consume 'BY'
            
            # Get provider name
            if self.match_token_type(TokenType.IDENTIFIER):
                provider = self.get_token_text(self.current_token())
                self.advance()
        
        # Parse SQL statements (collect everything until semicolon)
        sql_statements = []
        paren_depth = 0
        
        while not self.at_end() and not (self.match_token_type(TokenType.SEMI) and paren_depth == 0):
            token = self.current_token()
            token_text = self.get_token_text(token)
            
            if token_text == '(':
                paren_depth += 1
            elif token_text == ')':
                paren_depth -= 1
                
            sql_statements.append(token_text)
            self.advance()
        
        # Consume semicolon
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        execute_node = self.create_base_node(start_token, end_token)
        execute_node.update({
            'type': 'SasProcSqlExecuteStatement',
            'provider': provider,
            'sql_statements': ''.join(sql_statements).strip()
        })
        
        return execute_node
    
    def parse_sql_disconnect_statement(self) -> Dict[str, Any]:
        """Parse DISCONNECT FROM statement"""
        start_token = self.current_token()
        self.advance()  # consume 'DISCONNECT'
        
        # Expect 'FROM' 
        provider = "unknown"
        if self.match_token_type(TokenType.KW_FROM):
            self.advance()  # consume 'FROM'
            
            # Get provider name
            if self.match_token_type(TokenType.IDENTIFIER):
                provider = self.get_token_text(self.current_token())
                self.advance()
        
        # Consume semicolon
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        disconnect_node = self.create_base_node(start_token, end_token)
        disconnect_node.update({
            'type': 'SasProcSqlDisconnectStatement',
            'provider': provider
        })
        
        return disconnect_node
    
    def parse_sql_columns(self) -> List[Dict[str, Any]]:
        """Parse SELECT column list with complex expressions, CASE statements, functions"""
        columns = []
        
        while not self.at_end():
            # Stop at keywords that end column list
            if self.match_token_type(TokenType.KW_INTO) or \
               self.match_token_type(TokenType.KW_FROM) or \
               self.match_token_type(TokenType.SEMI):
                break
            
            # Parse individual column (which could be complex)
            column = self.parse_sql_column_expression()
            if column:
                columns.append(column)
            
            # Consume comma
            if self.match_token_type(TokenType.COMMA):
                self.advance()
            else:
                break
        
        return columns

    def parse_sql_column_expression(self) -> Dict[str, Any]:
        """Parse a single column expression (simple, CASE, function, calculated)"""
        start_token = self.current_token()
        
        # Check for CASE expression
        if self.match_token_type(TokenType.KW_CASE):
            return self.parse_sql_case_column()
        
        # Collect all tokens until comma, AS, or end keywords
        expression_tokens = []
        alias = None
        format_spec = None
        length_spec = None
        
        while not self.at_end():
            if (self.match_token_type(TokenType.COMMA) or
                self.match_token_type(TokenType.KW_FROM) or 
                self.match_token_type(TokenType.KW_INTO) or
                self.match_token_type(TokenType.SEMI)):
                break
            
            # Check for AS keyword for alias
            if self.match_token_type(TokenType.KW_AS):
                self.advance()  # consume 'as'
                if self.match_token_type(TokenType.IDENTIFIER):
                    alias = self.get_token_text(self.current_token())
                    self.advance()
                continue
            
            # Check for FORMAT specification
            token = self.current_token()
            token_text = self.get_token_text(token) if token else ""
            
            if token_text.lower().startswith('format='):
                format_spec = token_text[7:]  # Remove 'format='
                self.advance()
                continue
            elif token_text.lower() == 'format' and self.peek_token():
                next_token = self.peek_token()
                if self.get_token_text(next_token).startswith('='):
                    self.advance()  # consume 'format'
                    self.advance()  # consume '='
                    if not self.at_end():
                        format_spec = self.get_token_text(self.current_token())
                        self.advance()
                    continue
            
            # Check for LENGTH specification
            if token_text.lower().startswith('length='):
                length_spec = token_text[7:]  # Remove 'length='
                self.advance()
                continue
            elif token_text.lower() == 'length' and self.peek_token():
                next_token = self.peek_token()
                if self.get_token_text(next_token).startswith('='):
                    self.advance()  # consume 'length' 
                    self.advance()  # consume '='
                    if not self.at_end():
                        length_spec = self.get_token_text(self.current_token())
                        self.advance()
                    continue
            
            # Collect expression tokens
            if token:
                expression_tokens.append(token_text)
            self.advance()
        
        # Analyze the expression to determine type
        expression_text = ' '.join(expression_tokens).strip()
        
        # Determine column type based on content
        column_type = "SasSimpleColumn"
        if any(func in expression_text.lower() for func in ['length(', 'compress(', 'substr(', 'upcase(', 'cats(', 'datepart(', 'timepart(']):
            column_type = "SasFunctionColumn"
        elif any(op in expression_text for op in ['*', '+', '-', '/']):
            column_type = "SasCalculatedColumn"
        
        column_node = self.create_base_node(start_token, self.current_token() or start_token)
        column_node.update({
            'type': column_type,
            'expression': expression_text,
            'alias': alias,
            'format': format_spec,
            'length': length_spec,
            'raw_tokens': expression_tokens
        })
        
        return column_node

    def parse_sql_case_column(self) -> Dict[str, Any]:
        """Parse CASE WHEN expression as a column"""
        start_token = self.current_token()
        self.advance()  # consume 'case'
        
        when_clauses = []
        else_value = None
        
        while not self.at_end():
            if self.match_token_type(TokenType.KW_WHEN):
                self.advance()  # consume 'when'
                
                # Parse condition until THEN
                condition_tokens = []
                while (not self.at_end() and 
                       not self.match_token_type(TokenType.KW_THEN)):
                    token = self.current_token()
                    if token:
                        condition_tokens.append(self.get_token_text(token))
                    self.advance()
                
                condition = ' '.join(condition_tokens).strip()
                
                # Parse THEN value
                then_value = None
                if self.match_token_type(TokenType.KW_THEN):
                    self.advance()  # consume 'then'
                    
                    then_tokens = []
                    while (not self.at_end() and
                           not self.match_token_type(TokenType.KW_WHEN) and
                           not self.match_token_type(TokenType.KW_ELSE) and
                           not self.match_token_type(TokenType.KW_END)):
                        token = self.current_token()
                        if token:
                            then_tokens.append(self.get_token_text(token))
                        self.advance()
                    
                    then_value = ' '.join(then_tokens).strip()
                
                when_clauses.append({
                    'condition': condition,
                    'then_value': then_value
                })
            
            elif self.match_token_type(TokenType.KW_ELSE):
                self.advance()  # consume 'else'
                
                else_tokens = []
                while (not self.at_end() and
                       not self.match_token_type(TokenType.KW_END)):
                    token = self.current_token()
                    if token:
                        else_tokens.append(self.get_token_text(token))
                    self.advance()
                
                else_value = ' '.join(else_tokens).strip()
            
            elif self.match_token_type(TokenType.KW_END):
                self.advance()  # consume 'end'
                break
            else:
                self.advance()
        
        # Parse AS alias if present
        alias = None
        format_spec = None
        length_spec = None
        
        # Check for AS alias and format/length after END
        while not self.at_end():
            if (self.match_token_type(TokenType.COMMA) or
                self.match_token_type(TokenType.KW_FROM) or 
                self.match_token_type(TokenType.KW_INTO) or
                self.match_token_type(TokenType.SEMI)):
                break
                
            token = self.current_token()
            token_text = self.get_token_text(token) if token else ""
            
            if self.match_token_type(TokenType.KW_AS):
                self.advance()
                if self.match_token_type(TokenType.IDENTIFIER):
                    alias = self.get_token_text(self.current_token())
                    self.advance()
            elif token_text.lower().startswith('format='):
                format_spec = token_text[7:]
                self.advance()
            elif token_text.lower().startswith('length='):
                length_spec = token_text[7:]
                self.advance()
            else:
                self.advance()
        
        case_node = self.create_base_node(start_token, self.current_token() or start_token)
        case_node.update({
            'type': 'SasCaseColumn',
            'when_clauses': when_clauses,
            'else_value': else_value,
            'alias': alias,
            'format': format_spec,
            'length': length_spec
        })
        
        return case_node

    def parse_sql_into_clause(self) -> Dict[str, Any]:
        """Parse INTO clause (SAS specific)"""
        start_token = self.current_token()
        self.advance()  # consume 'into'
        
        # Collect INTO clause tokens
        into_tokens = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.KW_FROM) and
               not self.match_token_type(TokenType.SEMI)):
            token = self.current_token()
            if token:
                into_tokens.append(self.get_token_text(token))
            self.advance()
        
        into_text = ' '.join(into_tokens).strip()
        
        # Parse macro variables and separators
        macro_var = None
        separated_by = None
        
        if ':' in into_text:
            parts = into_text.split()
            for i, part in enumerate(parts):
                if part.startswith(':'):
                    macro_var = part
                elif part.lower() == 'separated' and i + 1 < len(parts) and parts[i + 1].lower() == 'by':
                    if i + 2 < len(parts):
                        separated_by = parts[i + 2].strip("'\"")
        
        into_node = self.create_base_node(start_token, start_token)
        into_node.update({
            'type': 'SasProcSqlIntoClause',
            'raw_text': into_text,
            'macro_variable': macro_var,
            'separated_by': separated_by
        })
        
        return into_node

    def parse_sql_from_clause_with_joins(self) -> Dict[str, Any]:
        """Parse FROM clause with INNER/LEFT/RIGHT JOINs"""
        start_token = self.current_token()
        self.advance()  # consume 'from'
        
        # Parse main table
        main_table = self.parse_sql_table_reference()
        
        # Parse JOINs
        joins = []
        while not self.at_end():
            # Check for JOIN keywords
            join_type = None
            if self.match_token_type(TokenType.KW_INNER):
                self.advance()
                if self.match_token_type(TokenType.KW_JOIN):
                    join_type = "INNER"
                    self.advance()
            elif self.match_token_type(TokenType.KW_LEFT):
                self.advance()
                if self.match_token_type(TokenType.KW_JOIN):
                    join_type = "LEFT"
                    self.advance()
            elif self.match_token_type(TokenType.KW_RIGHT):
                self.advance()
                if self.match_token_type(TokenType.KW_JOIN):
                    join_type = "RIGHT"
                    self.advance()
            elif self.match_token_type(TokenType.KW_FULL):
                self.advance()
                if self.match_token_type(TokenType.KW_JOIN):
                    join_type = "FULL"
                    self.advance()
            elif self.match_token_type(TokenType.KW_JOIN):
                join_type = "INNER"  # Default JOIN is INNER
                self.advance()
            else:
                break  # No more JOINs
            
            if join_type:
                # Parse joined table
                joined_table = self.parse_sql_table_reference()
                
                # Parse ON condition
                on_condition = None
                if self.match_token_type(TokenType.KW_ON):
                    on_condition = self.parse_sql_join_condition()
                
                join_info = {
                    'type': 'SasProcSqlJoin',
                    'join_type': join_type,
                    'table': joined_table,
                    'on_condition': on_condition
                }
                joins.append(join_info)
        
        from_node = self.create_base_node(start_token, start_token)
        from_node.update({
            'type': 'SasProcSqlFromClause',
            'main_table': main_table,
            'joins': joins
        })
        
        return from_node

    def parse_sql_table_reference(self) -> Dict[str, Any]:
        """Parse table reference with optional alias (supports macro variables like &DATA_SAMPLE.)"""
        table_name = "unknown"
        alias = None
        
        # Parse table name - handle macro variables and regular identifiers
        if self.match_token_type(TokenType.MACRO_VAR_RESOLVE):
            # Handle macro variable: &DATA_SAMPLE.
            table_name = "&"  # MACRO_VAR_RESOLVE is the &
            self.advance()
            
            if self.match_token_type(TokenType.MACRO_STRING):
                table_name += self.get_token_text(self.current_token())  # &DATA_SAMPLE
                self.advance()
                
                # Handle optional MACRO_VAR_TERM for macro variable termination
                if self.match_token_type(TokenType.MACRO_VAR_TERM):
                    table_name += "."  # MACRO_VAR_TERM is the .
                    self.advance()
                    
            # Check for alias after macro variable - can be with or without AS keyword
            if self.match_token_type(TokenType.KW_AS):
                self.advance()  # consume 'as'
                if self.match_token_type(TokenType.IDENTIFIER):
                    alias = self.get_token_text(self.current_token())
                    self.advance()
            elif self.match_token_type(TokenType.IDENTIFIER):
                # Check if this identifier is actually an alias (not a JOIN keyword)
                identifier_text = self.get_token_text(self.current_token()).upper()
                # Don't treat JOIN keywords as aliases
                if identifier_text not in ['INNER', 'LEFT', 'RIGHT', 'FULL', 'JOIN', 'ON', 'WHERE', 'GROUP', 'ORDER', 'HAVING']:
                    alias = self.get_token_text(self.current_token())
                    self.advance()
                
        elif self.match_token_type(TokenType.IDENTIFIER):
            # Handle regular identifier or libref.table
            table_name = self.get_token_text(self.current_token())
            self.advance()
            
            if self.match_token_type(TokenType.DOT):
                self.advance()
                if self.match_token_type(TokenType.IDENTIFIER):
                    table_name += "." + self.get_token_text(self.current_token())
                    self.advance()
            
            # Check for alias - can be with or without AS keyword
            if self.match_token_type(TokenType.KW_AS):
                self.advance()  # consume 'as'
                if self.match_token_type(TokenType.IDENTIFIER):
                    alias = self.get_token_text(self.current_token())
                    self.advance()
            elif self.match_token_type(TokenType.IDENTIFIER):
                # Check if this identifier is actually an alias (not a JOIN keyword)
                identifier_text = self.get_token_text(self.current_token()).upper()
                # Don't treat JOIN keywords as aliases
                if identifier_text not in ['INNER', 'LEFT', 'RIGHT', 'FULL', 'JOIN', 'ON', 'WHERE', 'GROUP', 'ORDER', 'HAVING']:
                    alias = self.get_token_text(self.current_token())
                    self.advance()
        
        return {
            'type': 'SasProcSqlTableRef',
            'table_name': table_name,
            'alias': alias
        }

    def parse_sql_join_condition(self) -> Dict[str, Any]:
        """Parse JOIN ON condition"""
        start_token = self.current_token()
        self.advance()  # consume 'on'
        
        # Collect condition tokens until next keyword
        condition_tokens = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.KW_INNER) and
               not self.match_token_type(TokenType.KW_LEFT) and
               not self.match_token_type(TokenType.KW_RIGHT) and
               not self.match_token_type(TokenType.KW_JOIN) and
               not self.match_token_type(TokenType.KW_WHERE) and
               not self.match_token_type(TokenType.KW_GROUP) and
               not self.match_token_type(TokenType.KW_ORDER) and
               not self.match_token_type(TokenType.SEMI)):
            
            token = self.current_token()
            if token:
                condition_tokens.append(self.get_token_text(token))
            self.advance()
        
        condition = ' '.join(condition_tokens).strip()
        
        return {
            'type': 'SasProcSqlJoinCondition',
            'condition': condition
        }
    
    def parse_proc_by_statement(self) -> Dict[str, Any]:
        """Parse BY statement in PROC"""
        start_token = self.current_token()
        self.advance()  # consume 'by'
        
        variables = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI)):
            if self.match_token_type(TokenType.IDENTIFIER):
                variables.append(self.get_token_text(self.current_token()))
            self.advance()
        
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        by_node = self.create_base_node(start_token, end_token)
        by_node.update({
            'type': 'SasProcByStatement',
            'variables': variables
        })
        
        return by_node
    
    def parse_proc_var_statement(self) -> Dict[str, Any]:
        """Parse VAR statement in PROC"""
        start_token = self.current_token()
        self.advance()  # consume 'var'
        
        variables = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI)):
            if self.match_token_type(TokenType.IDENTIFIER):
                variables.append(self.get_token_text(self.current_token()))
            self.advance()
        
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        var_node = self.create_base_node(start_token, end_token)
        var_node.update({
            'type': 'SasProcVarStatement',
            'variables': variables
        })
        
        return var_node

    def parse_macro_identifier_call(self) -> Optional[Dict[str, Any]]:
        """Parse macro call from MACRO_IDENTIFIER token like %outer1"""
        start_token = self.current_token()
        if not self.match_token_type(TokenType.MACRO_IDENTIFIER):
            return None

        # Extract macro name from the MACRO_IDENTIFIER token (remove the %)
        macro_text = self.get_token_text(start_token)
        macro_name = macro_text[1:] if macro_text.startswith('%') else macro_text
        
        # Check if this is a macro language keyword that should NOT be treated as a call
        macro_keywords = {
            'mend', 'macro', 'if', 'then', 'else', 'do', 'end', 'let', 'include',
            'put', 'return', 'abort', 'global', 'local', 'to', 'by', 'until', 'while'
        }
        
        if macro_name.lower() in macro_keywords:
            print(f"[DEBUG] Skipping macro keyword %{macro_name} at pos {self.pos}")
            return None  # Not a macro call, it's a macro language keyword

        self.advance()  # consume MACRO_IDENTIFIER

        # Parse arguments if present
        args = []
        if self.match_token_type(TokenType.LPAREN):
            self.advance()  # consume '('

            # Collect arguments until ')'
            while not self.at_end() and not self.match_token_type(TokenType.RPAREN):
                token = self.current_token()
                if token:
                    args.append(self.get_token_text(token))
                self.advance()

            if self.match_token_type(TokenType.RPAREN):
                self.advance()  # consume ')'

        # Consume semicolon if present
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        else:
            end_token = start_token

        base_node = self.create_base_node(start_token, end_token)
        base_node.update({
            'type': 'SasMacroCall',
            'macro_name': macro_name,
            'arguments': {
                'positional': args,
                'keyword': {},
                'raw_order': args
            }
        })

        return base_node

    def parse_bare_macro_call(self) -> Optional[Dict[str, Any]]:
        """Parse bare macro call from IDENTIFIER + SEMI tokens like inner2;"""
        start_token = self.current_token()
        if not self.match_token_type(TokenType.IDENTIFIER):
            return None
        
        # Get macro name
        macro_name = self.get_token_text(start_token)
        self.advance()  # consume IDENTIFIER
        
        # Consume semicolon
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        base_node = self.create_base_node(start_token, end_token)
        base_node.update({
            'type': 'SasMacroCall',
            'macro_name': macro_name,
            'arguments': {
                'positional': [],
                'keyword': {},
                'raw_order': []
            }
        })
        
        return base_node

    def parse_macro_assignment_statement(self) -> Dict[str, Any]:
        """Parse assignment statement within macro body that may contain macro variables"""
        start_token = self.current_token()
        
        # Parse variable name (may contain macro variables like NumSearches_&type)
        variable_tokens = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.ASSIGN)):
            token = self.current_token()
            variable_tokens.append(self.get_token_text(token))
            self.advance()
        
        variable_name = ''.join(variable_tokens)
        
        # Consume assignment operator
        if self.match_token_type(TokenType.ASSIGN):
            self.advance()
        
        # Parse expression (simplified - collect until semicolon)
        expression_tokens = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI)):
            expression_tokens.append(self.get_token_text(self.current_token()))
            self.advance()
        
        expression = ''.join(expression_tokens).strip()
        
        # Consume semicolon
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        assignment_node = self.create_base_node(start_token, end_token)
        assignment_node.update({
            'type': 'SasMacroAssignmentStatement',
            'variable': variable_name,
            'expression': expression
        })
        
        return assignment_node
    
    def parse_format_statement(self) -> Dict[str, Any]:
        """Parse FORMAT statement"""
        start_token = self.current_token()
        self.advance()  # consume 'format'
        
        # Collect all tokens until semicolon
        format_tokens = []
        while (not self.at_end() and 
               not self.match_token_type(TokenType.SEMI)):
            token = self.current_token()
            format_tokens.append(self.get_token_text(token))
            self.advance()
        
        format_text = ''.join(format_tokens).strip()
        
        # Consume semicolon
        end_token = start_token
        if self.match_token_type(TokenType.SEMI):
            end_token = self.current_token()
            self.advance()
        
        format_node = self.create_base_node(start_token, end_token)
        format_node.update({
            'type': 'SasFormatStatement',
            'format_specification': format_text
        })
        
        return format_node

    def _is_known_macro_name(self, name: str) -> bool:
        """Check if identifier is a known macro name"""
        # For now, we'll use a heuristic approach since we're parsing in one pass
        # In a real implementation, you'd want a two-pass parser:
        # Pass 1: Collect all macro definitions
        # Pass 2: Resolve calls using the symbol table
        
        # Simple heuristic: if it's not a common SAS keyword, assume it could be a macro
        sas_keywords = {
            'data', 'proc', 'run', 'quit', 'if', 'then', 'else', 'do', 'end',
            'set', 'merge', 'by', 'where', 'output', 'delete', 'drop', 'keep',
            'select', 'from', 'create', 'table', 'insert', 'update', 'group',
            'order', 'having', 'union', 'join', 'on', 'as', 'distinct'
        }
        
        # Also exclude common SAS function names
        sas_functions = {
            'length', 'substr', 'compress', 'upcase', 'lowcase', 'trim', 'strip',
            'cats', 'catx', 'scan', 'index', 'find', 'tranwrd', 'translate'
        }
        
        name_lower = name.lower()
        
        # Not a keyword or function, and looks like an identifier
        return (name_lower not in sas_keywords and 
                name_lower not in sas_functions and
                name.isidentifier() and
                len(name) > 1)


def parse_sas_code(code: str, filename: str = "") -> Dict[str, Any]:
    """
    Main entry point - parse SAS code string into YAML schema format
    """
    tokens, errors, str_lit_buf = lex_program_from_str(code)
    
    parser = SasParser(tokens, code, filename)
    module = parser.parse_module()
    
    # Add lexer errors if any
    if errors:
        module.setdefault('errors', []).extend([str(e) for e in errors])
    
    return module
