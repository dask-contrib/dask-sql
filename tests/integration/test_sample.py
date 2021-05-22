from pandas.testing import assert_frame_equal


def test_sample(c, df):
    # Fixed sample, check absolute numbers
    return_df = c.sql("SELECT * FROM df TABLESAMPLE SYSTEM (20) REPEATABLE (10)")
    return_df = return_df.compute()

    assert len(return_df) == 234

    return_df = c.sql("SELECT * FROM df TABLESAMPLE SYSTEM (20) REPEATABLE (11)")
    return_df = return_df.compute()

    assert len(return_df) == 468  # Yes, that is horrible, but at least fast...

    return_df = c.sql("SELECT * FROM df TABLESAMPLE SYSTEM (50) REPEATABLE (10)")
    return_df = return_df.compute()

    assert len(return_df) == 234

    return_df = c.sql("SELECT * FROM df TABLESAMPLE SYSTEM (0.001) REPEATABLE (10)")
    return_df = return_df.compute()

    assert len(return_df) == 0

    return_df = c.sql("SELECT * FROM df TABLESAMPLE SYSTEM (99.999) REPEATABLE (10)")
    return_df = return_df.compute()

    assert len(return_df) == len(df)

    return_df = c.sql("SELECT * FROM df TABLESAMPLE BERNOULLI (50) REPEATABLE (10)")
    return_df = return_df.compute()

    assert len(return_df) == 350

    return_df = c.sql("SELECT * FROM df TABLESAMPLE BERNOULLI (70) REPEATABLE (10)")
    return_df = return_df.compute()

    assert len(return_df) == 490

    return_df = c.sql("SELECT * FROM df TABLESAMPLE BERNOULLI (0.001) REPEATABLE (10)")
    return_df = return_df.compute()

    assert len(return_df) == 0

    return_df = c.sql("SELECT * FROM df TABLESAMPLE BERNOULLI (99.999) REPEATABLE (10)")
    return_df = return_df.compute()

    assert len(return_df) == len(df)

    # Not fixed sample, can only check boundaries
    return_df = c.sql("SELECT * FROM df TABLESAMPLE BERNOULLI (50)")
    return_df = return_df.compute()

    assert len(return_df) >= 0 and len(return_df) <= len(df)

    return_df = c.sql("SELECT * FROM df TABLESAMPLE SYSTEM (50)")
    return_df = return_df.compute()

    assert len(return_df) >= 0 and len(return_df) <= len(df)
