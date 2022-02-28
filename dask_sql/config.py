import os

import dask
import yaml

fn = os.path.join(os.path.dirname(__file__), "sql.yaml")

with open(fn) as f:
    defaults = yaml.safe_load(f)

dask.config.update_defaults(defaults)
dask.config.ensure_file(source=fn, comment=True)
