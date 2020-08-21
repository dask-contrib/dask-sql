class RexInputRefPlugin:
    class_name = "org.apache.calcite.rex.RexInputRef"

    def __call__(self, rex, df):
        index = rex.getIndex()
        return df.iloc[:, index]
