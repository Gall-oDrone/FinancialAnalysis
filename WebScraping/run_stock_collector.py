#!/usr/bin/env python
"""
Script to run StockCollector notebook with progress tracking.
This allows you to see output in real-time.
"""

import sys
import os

# Add paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../Storage'))

from nbclient import NotebookClient
import nbformat
from datetime import datetime

def run_notebook():
    """Execute the StockCollector notebook."""
    notebook_path = os.path.join(os.path.dirname(__file__), 'notebooks/StockCollector.ipynb')
    
    print("=" * 70)
    print(f"Executing StockCollector Notebook")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print(f"Notebook: {notebook_path}")
    print()
    
    # Read the notebook
    with open(notebook_path, 'r', encoding='utf-8') as f:
        nb = nbformat.read(f, as_version=4)
    
    # Execute the notebook
    client = NotebookClient(
        nb,
        timeout=7200,  # 2 hours timeout (since processing all books)
        kernel_name='python3',
        resources={'metadata': {'path': os.path.dirname(notebook_path)}},
        allow_errors=True  # Continue on errors
    )
    
    try:
        print("Starting execution...")
        print("Note: This may take a long time if processing all books.")
        print("Press Ctrl+C to stop (notebook will be saved with partial results)")
        print("-" * 70)
        print()
        
        client.execute()
        
        print()
        print("=" * 70)
        print(f"Notebook executed successfully!")
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        
    except KeyboardInterrupt:
        print()
        print("=" * 70)
        print("Execution interrupted by user")
        print("Saving notebook with partial results...")
        print("=" * 70)
        
    except Exception as e:
        print()
        print("=" * 70)
        print(f"Error during execution: {e}")
        print("Saving notebook with partial results...")
        import traceback
        traceback.print_exc()
        print("=" * 70)
        
    finally:
        # Save the executed notebook
        print("Saving executed notebook...")
        with open(notebook_path, 'w', encoding='utf-8') as f:
            nbformat.write(nb, f)
        print(f"Notebook saved to: {notebook_path}")
        print()
        print("=" * 70)
        print("Done!")
        print("=" * 70)

if __name__ == "__main__":
    run_notebook()
