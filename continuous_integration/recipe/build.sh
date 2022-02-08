# This assumes the script is executed from the root of the repo directory
$PYTHON setup.py build_ext
$PYTHON -m pip install . --no-deps -vv
