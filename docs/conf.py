import os
import sys

# Point to the source code
sys.path.insert(0, os.path.abspath('../src'))

# Project information
project = 'Avian Biosurveillance'
copyright = '2026, Soheil'
author = 'Soheil'
release = '0.1.0'

# Extensions
extensions = [
    'sphinx.ext.autodoc',      # Automatic documentation from docstrings
    'sphinx.ext.viewcode',     # Add links to highlighted source code
    'sphinx.ext.napoleon',     # Support for Google and NumPy style docstrings
    'sphinx.ext.todo',         # Support for todo items
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# Theme
html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
