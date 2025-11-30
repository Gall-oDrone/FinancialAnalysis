#!/usr/bin/env python3
"""Script to execute a Jupyter notebook in Docker container."""
import sys
import os

# Add user's local site-packages to path
local_site_packages = os.path.expanduser("~/.local/lib/python3.11/site-packages")
if os.path.exists(local_site_packages):
    sys.path.insert(0, local_site_packages)

try:
    from nbconvert import PythonExporter
    import nbformat
except ImportError as e:
    print(f"Error importing nbconvert: {e}")
    print("Attempting to install...")
    os.system("pip install --user nbconvert")
    from nbconvert import PythonExporter
    import nbformat

def execute_notebook(notebook_path):
    """Execute a Jupyter notebook."""
    print(f"Executing notebook: {notebook_path}")
    
    # Read notebook
    with open(notebook_path, 'r', encoding='utf-8') as f:
        nb = nbformat.read(f, as_version=4)
    
    # Convert to Python and execute
    exporter = PythonExporter()
    (body, resources) = exporter.from_notebook_node(nb)
    
    # Write to temp file and execute
    script_path = "/tmp/notebook_script.py"
    with open(script_path, 'w', encoding='utf-8') as f:
        f.write(body)
    
    print(f"Converted notebook to: {script_path}")
    print("Executing script...")
    print("=" * 60)
    
    # Execute the script
    exec(compile(body, script_path, 'exec'), globals(), globals())
    
    print("=" * 60)
    print("Notebook execution completed!")

if __name__ == "__main__":
    notebook_path = sys.argv[1] if len(sys.argv) > 1 else "WebScraping/notebooks/NewsCollector-Staging.ipynb"
    execute_notebook(notebook_path)


