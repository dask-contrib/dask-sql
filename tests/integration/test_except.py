def test_except_empty(c, df):
    result_df = c.sql(
        """
        SELECT * FROM df
        EXCEPT
        SELECT * FROM df
        """
    )
    result_df = result_df.compute()
    assert len(result_df) == 0


def test_except_non_empty(c, df):
    result_df = c.sql(
        """
        (
            SELECT 1 as "a"
            UNION
            SELECT 2 as "a"
            UNION
            SELECT 3 as "a"
        )
        EXCEPT
        SELECT 2 as "a"
        """
    )
    result_df = result_df.compute()
    assert result_df.columns == "a"
    assert set(result_df["a"]) == set([1, 3])
