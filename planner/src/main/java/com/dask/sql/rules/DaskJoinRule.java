package com.dask.sql.rules;

import com.dask.sql.nodes.DaskRel;
import com.dask.sql.nodes.DaskJoin;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;

public class DaskJoinRule extends ConverterRule {
    public static final DaskJoinRule INSTANCE = Config.INSTANCE
            .withConversion(LogicalJoin.class, Convention.NONE, DaskRel.CONVENTION, "DaskJoinRule")
            .withRuleFactory(DaskJoinRule::new).toRule(DaskJoinRule.class);

    DaskJoinRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalJoin join = (LogicalJoin) rel;
        final RelTraitSet traitSet = join.getTraitSet().replace(out);
        RelNode transformedLeft = convert(join.getLeft(), join.getLeft().getTraitSet().replace(DaskRel.CONVENTION));
        RelNode transformedRight = convert(join.getRight(), join.getRight().getTraitSet().replace(DaskRel.CONVENTION));

        return DaskJoin.create(join.getCluster(), traitSet, transformedLeft, transformedRight, join.getCondition(),
                join.getVariablesSet(), join.getJoinType());
    }
}
