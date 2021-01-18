import logging

import numpy as np

from dask_sql.physical.rel.base import BaseRelPlugin
from dask_sql.datacontainer import DataContainer


logger = logging.getLogger(__name__)


class SamplePlugin(BaseRelPlugin):
    """
    Sample is used on TABLESAMPLE clauses.
    It returns only a fraction of the table, given by the
    number in the arguments.
    There exist two algorithms, SYSTEM or BERNOULLI.

    SYSTEM is a very fast algorithm, which works on partition
    level: a partition is kept with a probability given by the
    percentage. This algorithm will - especially for very small
    numbers of partitions - give wrong results. Only choose
    it when you really have too much data to apply BERNOULLI
    (which might never be the case in real world applications).

    BERNOULLI samples each row separately and will still
    give only an approximate fraction, but much closer to
    the expected.
    """

    class_name = "org.apache.calcite.rel.core.Sample"

    def convert(
        self, rel: "org.apache.calcite.rel.RelNode", context: "dask_sql.Context"
    ) -> DataContainer:
        (dc,) = self.assert_inputs(rel, 1, context)
        df = dc.df
        cc = dc.column_container

        parameters = rel.getSamplingParameters()
        is_bernoulli = parameters.isBernoulli()
        fraction = float(parameters.getSamplingPercentage())
        seed = parameters.getRepeatableSeed() if parameters.isRepeatable() else None

        if is_bernoulli:
            df = df.sample(frac=fraction, replace=False, random_state=seed)
        else:
            random_state = np.random.RandomState(seed)
            random_choice = random_state.choice(
                [True, False],
                size=df.npartitions,
                replace=True,
                p=[fraction, 1 - fraction],
            )

            if random_choice.any():
                df = df.partitions[random_choice]
            else:
                df = df.head(0, compute=False)

        return DataContainer(df, cc)
