#!/usr/bin/env python3
"""
Join multi-line LOG() calls onto single lines for LCOV coverage filtering.

This script is used only during coverage builds to simplify LOG statement exclusion.
It doesn't modify the original source files - it works on copies during the build.
Adjacent string literals are concatenated at compile time, so runtime behavior is identical.

Uses a proper C++ tokenizer to handle string literals, character literals, and comments
correctly, avoiding issues with unbalanced parentheses or semicolons in strings.
"""

import re
import sys
from pathlib import Path


_LOG_MACRO_PATTERN = re.compile(r'\bLOG[A-Z_]*\s*\(')


def _find_log_macro_open(line: str):
    """Return the index of the opening parenthesis for a LOG-like macro, if present."""
    match = _LOG_MACRO_PATTERN.search(line)
    if not match:
        return None
    return match.end() - 1


def _find_log_statement_end(lines, start_line, open_paren_index):
    """Find the line index where the LOG macro call closes, ignoring literals/comments.
    
    This properly handles:
    - String literals: LOG("unbalanced (", x);
    - Character literals: LOG(')', code);  
    - Line comments: LOG("msg", x);  // comment with )
    - Block comments: LOG("msg" /* comment ) */, x);
    """
    depth = 0
    in_string = False
    in_char = False
    in_block_comment = False
    escape = False
    
    for line_index in range(start_line, len(lines)):
        line = lines[line_index]
        i = open_paren_index if line_index == start_line else 0
        in_line_comment = False
        
        while i < len(line):
            ch = line[i]
            nxt = line[i + 1] if i + 1 < len(line) else ''
            
            # Line comments consume rest of line
            if in_line_comment:
                break
            
            # Inside block comment - only look for */
            if in_block_comment:
                if ch == '*' and nxt == '/':
                    in_block_comment = False
                    i += 2
                    continue
                i += 1
                continue
            
            # Inside string literal - handle escapes
            if in_string:
                if escape:
                    escape = False
                elif ch == '\\':
                    escape = True
                elif ch == '"':
                    in_string = False
                i += 1
                continue
            
            # Inside character literal - handle escapes
            if in_char:
                if escape:
                    escape = False
                elif ch == '\\':
                    escape = True
                elif ch == "'":
                    in_char = False
                i += 1
                continue
            
            # Check for comment starts
            if ch == '/' and nxt == '/':
                in_line_comment = True
                break
            if ch == '/' and nxt == '*':
                in_block_comment = True
                i += 2
                continue
            
            # Check for literal starts
            if ch == '"':
                in_string = True
                escape = False
                i += 1
                continue
            if ch == "'":
                in_char = True
                escape = False
                i += 1
                continue
            
            # Count parentheses depth outside of literals/comments
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    return line_index
            
            i += 1
    
    return None


def join_log_statements(content: str) -> str:
    """Join multi-line LOG macro calls onto a single line using proper C++ tokenization."""
    lines = content.split('\n')
    result = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Check if this line contains a LOG macro start
        open_paren_index = _find_log_macro_open(line)
        if open_paren_index is not None:
            # Find where the LOG statement ends, respecting C++ syntax
            end_index = _find_log_statement_end(lines, i, open_paren_index)
            if end_index is not None and end_index > i:
                # Multi-line LOG statement found - join it
                full_statement = lines[i]
                for join_index in range(i + 1, end_index + 1):
                    full_statement += ' ' + lines[join_index].strip()
                result.append(full_statement)
                i = end_index + 1
                continue
        
        # Not a LOG statement or single-line LOG - keep as is
        result.append(line)
        i += 1
    
    return '\n'.join(result)


def process_file(filepath: Path) -> None:
    """Process a single C++ source file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        modified = join_log_statements(content)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(modified)
        
        print(f"[INFO] Processed: {filepath}")
    except Exception as e:
        print(f"[ERROR] Failed to process {filepath}: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    """Process all .cpp and .hpp files in the pybind directory."""
    if len(sys.argv) > 1:
        # Process specific directory passed as argument
        base_dir = Path(sys.argv[1])
    else:
        # Default to current directory
        base_dir = Path.cwd()
    
    if not base_dir.exists():
        print(f"[ERROR] Directory not found: {base_dir}", file=sys.stderr)
        sys.exit(1)
    
    # Find all C++ source files
    cpp_files = list(base_dir.rglob('*.cpp')) + list(base_dir.rglob('*.hpp'))
    
    if not cpp_files:
        print(f"[WARNING] No .cpp or .hpp files found in {base_dir}")
        return
    
    print(f"[INFO] Processing {len(cpp_files)} C++ files in {base_dir}")
    for filepath in cpp_files:
        process_file(filepath)
    
    print(f"[SUCCESS] Joined LOG statements in {len(cpp_files)} files")


if __name__ == '__main__':
    main()
