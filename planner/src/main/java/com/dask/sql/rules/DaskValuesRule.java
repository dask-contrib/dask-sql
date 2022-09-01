package com.dask.sql.rules;

import com.dask.sql.nodes.DaskRel;
import com.dask.sql.nodes.DaskValues;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalValues;

public class DaskValuesRule extends ConverterRule {
    public static final DaskValuesRule INSTANCE = Config.INSTANCE
            .withConversion(LogicalValues.class, Convention.NONE, DaskRel.CONVENTION, "DaskValuesRule")
            .withRuleFactory(DaskValuesRule::new).toRule(DaskValuesRule.class);

    DaskValuesRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalValues values = (LogicalValues) rel;
        final RelTraitSet traitSet = values.getTraitSet().replace(out);
        return DaskValues.create(values.getCluster(), values.getRowType(), values.getTuples(), traitSet);
    }
}
