from unittest import TestCase
import warnings

import dask.dataframe as dd
import pandas as pd
from pandas.testing import assert_frame_equal

from dask_sql import Context


class ContextTestCase(TestCase):
    def test_add_remove_tables(self):
        c = Context()

        data_frame = dd.from_pandas(pd.DataFrame(), npartitions=1)

        c.create_table("table", data_frame)
        self.assertIn("table", c.tables)

        c.drop_table("table")
        self.assertNotIn("table", c.tables)

        self.assertRaises(KeyError, c.drop_table, "table")

    def test_deprecation_warning(self):
        c = Context()
        data_frame = dd.from_pandas(pd.DataFrame(), npartitions=1)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            c.register_dask_table(data_frame, "table")

            assert len(w) == 1
            assert issubclass(w[-1].category, DeprecationWarning)

        self.assertIn("table", c.tables)

        c.drop_table("table")
        self.assertNotIn("table", c.tables)

    def test_explain(self):
        c = Context()

        data_frame = dd.from_pandas(pd.DataFrame({"a": [1, 2, 3]}), npartitions=1)
        c.create_table("df", data_frame)

        sql_string = c.explain("SELECT * FROM df")

        self.assertEqual(
            sql_string,
            "LogicalProject(a=[$0])\n  LogicalTableScan(table=[[schema, df]])\n",
        )

    def test_sql(self):
        c = Context()

        data_frame = dd.from_pandas(pd.DataFrame({"a": [1, 2, 3]}), npartitions=1)
        c.create_table("df", data_frame)

        result = c.sql("SELECT * FROM df")
        self.assertIsInstance(result, dd.DataFrame)

        result = c.sql("SELECT * FROM df", return_futures=False)
        self.assertIsInstance(result, pd.DataFrame)

    def test_input_types(self):
        c = Context()
        df = pd.DataFrame({"a": [1, 2, 3]})

        def assert_correct_output():
            result = c.sql("SELECT * FROM df")
            self.assertIsInstance(result, dd.DataFrame)
            assert_frame_equal(result.compute(), df)

        c.create_table("df", df)
        assert_correct_output()

        c.create_table("df", dd.from_pandas(df, npartitions=1))
        assert_correct_output()

