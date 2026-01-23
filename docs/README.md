
### How to Use

To rebuild the documentation in the future (e.g., after adding new code):

```
# 1. Update the API reference files (scans src/ for changes)
poetry run sphinx-apidoc -o docs src

# 2. Build the HTML website
poetry run sphinx-build -b html docs docs/_build/html
```

You can view the documentation by opening this file in your browser:                                                                                     
docs/_build/html/index.html         