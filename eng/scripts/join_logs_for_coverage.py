#!/usr/bin/env python3
"""
Join multi-line LOG() calls onto single lines for LCOV coverage filtering.

This script is used only during coverage builds to simplify LOG statement exclusion.
It doesn't modify the original source files - it works on copies during the build.
Adjacent string literals are concatenated at compile time, so runtime behavior is identical.
"""

import re
import sys
from pathlib import Path


def join_log_statements(content: str) -> str:
    """Join multi-line LOG macro calls onto a single line."""
    lines = content.split('\n')
    result = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Check if this line contains a LOG macro start
        if re.search(r'\bLOG[A-Z_]*\s*\(', line):
            # Start collecting the full statement
            full_statement = line
            paren_depth = line.count('(') - line.count(')')
            i += 1
            
            # Continue collecting until we close all parentheses
            while i < len(lines) and paren_depth > 0:
                next_line = lines[i]
                full_statement += ' ' + next_line.strip()
                paren_depth += next_line.count('(') - next_line.count(')')
                i += 1
            
            # Add the joined statement
            result.append(full_statement)
        else:
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
