Sample Module Repository
========================

This simple project is an example repo for Python projects.

---------------

Install package
````
pip install -r requirements.txt
````


Build package
````
python setup.py sdist
````

Test Package
````
pytest tests
````

Run Package
export PYTHONPATH=.
python cluster/TreeQueryExecute.py resource/TreeQueryInput3.json