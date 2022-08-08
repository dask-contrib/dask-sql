import numpy as np

from tests.utils import assert_eq


def get_system_sample(df, fraction, seed):
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

    return df


def test_sample(c, df):
    ddf = c.sql("SELECT * FROM df")

    # fixed system samples
    assert_eq(
        c.sql("SELECT * FROM df TABLESAMPLE SYSTEM (20) REPEATABLE (10)"),
        get_system_sample(ddf, 0.20, 10),
    )
    assert_eq(
        c.sql("SELECT * FROM df TABLESAMPLE SYSTEM (20) REPEATABLE (11)"),
        get_system_sample(ddf, 0.20, 11),
    )
    assert_eq(
        c.sql("SELECT * FROM df TABLESAMPLE SYSTEM (50) REPEATABLE (10)"),
        get_system_sample(ddf, 0.50, 10),
    )
    assert_eq(
        c.sql("SELECT * FROM df TABLESAMPLE SYSTEM (0.001) REPEATABLE (10)"),
        get_system_sample(ddf, 0.00001, 10),
    )
    assert_eq(
        c.sql("SELECT * FROM df TABLESAMPLE SYSTEM (99.999) REPEATABLE (10)"),
        get_system_sample(ddf, 0.99999, 10),
    )

    # fixed bernoulli samples
    assert_eq(
        c.sql("SELECT * FROM df TABLESAMPLE BERNOULLI (50) REPEATABLE (10)"),
        ddf.sample(frac=0.50, replace=False, random_state=10),
    )
    assert_eq(
        c.sql("SELECT * FROM df TABLESAMPLE BERNOULLI (70) REPEATABLE (10)"),
        ddf.sample(frac=0.70, replace=False, random_state=10),
    )
    assert_eq(
        c.sql("SELECT * FROM df TABLESAMPLE BERNOULLI (0.001) REPEATABLE (10)"),
        ddf.sample(frac=0.00001, replace=False, random_state=10),
    )
    assert_eq(
        c.sql("SELECT * FROM df TABLESAMPLE BERNOULLI (99.999) REPEATABLE (10)"),
        ddf.sample(frac=0.99999, replace=False, random_state=10),
    )

    # variable samples, can only check boundaries
    return_df = c.sql("SELECT * FROM df TABLESAMPLE BERNOULLI (50)")
    assert len(return_df) >= 0 and len(return_df) <= len(df)

    return_df = c.sql("SELECT * FROM df TABLESAMPLE SYSTEM (50)")
    assert len(return_df) >= 0 and len(return_df) <= len(df)
