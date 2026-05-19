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
    """Join multi-line LOG macro calls onto a single line.
    
    Uses semicolon-based joining rather than parenthesis counting to avoid
    issues with unbalanced parentheses inside C++ string literals.
    """
    lines = content.split('\n')
    result = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Check if this line contains a LOG macro start
        if re.search(r'\bLOG[A-Z_]*\s*\(', line):
            # Start collecting the full statement until we find a semicolon
            full_statement = line
            start_line = i + 1  # For error reporting
            i += 1
            lines_collected = 1
            max_lines = 20  # Safety limit to prevent runaway joins
            
            # Continue collecting until we find a semicolon (end of statement)
            # This is more reliable than parenthesis counting for LOG statements
            # because it doesn't get confused by unbalanced parens in string literals
            while i < len(lines) and ';' not in full_statement and lines_collected < max_lines:
                next_line = lines[i]
                full_statement += ' ' + next_line.strip()
                i += 1
                lines_collected += 1
            
            # Validation: Check if we found a semicolon
            if ';' not in full_statement:
                print(f"[WARNING] No semicolon found in LOG statement starting at line {start_line}")
                print(f"[WARNING] Collected {lines_collected} lines without finding ';'")
                print(f"[WARNING] First 100 chars: {full_statement[:100]}...")
                # Keep the original multi-line format for safety
                result.extend(lines[start_line-1:i])
            else:
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
