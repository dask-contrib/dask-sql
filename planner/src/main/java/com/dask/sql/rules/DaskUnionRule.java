package com.dask.sql.rules;

import java.util.List;
import java.util.stream.Collectors;

import com.dask.sql.nodes.DaskRel;
import com.dask.sql.nodes.DaskUnion;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalUnion;

public class DaskUnionRule extends ConverterRule {
    public static final DaskUnionRule INSTANCE = Config.INSTANCE
            .withConversion(LogicalUnion.class, Convention.NONE, DaskRel.CONVENTION, "DaskUnionRule")
            .withRuleFactory(DaskUnionRule::new).toRule(DaskUnionRule.class);

    DaskUnionRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalUnion union = (LogicalUnion) rel;
        final RelTraitSet traitSet = union.getTraitSet().replace(out);
        List<RelNode> transformedInputs = union.getInputs().stream()
                .map(c -> convert(c, c.getTraitSet().replace(DaskRel.CONVENTION))).collect(Collectors.toList());
        return DaskUnion.create(union.getCluster(), traitSet, transformedInputs, union.all);
    }
}
