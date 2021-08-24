package com.dask.sql.rules;

import com.dask.sql.nodes.DaskRel;
import com.dask.sql.nodes.DaskWindow;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.logical.LogicalWindow;

public class DaskWindowRule extends ConverterRule {
    public static final DaskWindowRule INSTANCE = Config.INSTANCE
            .withConversion(LogicalWindow.class, Convention.NONE, DaskRel.CONVENTION, "DaskWindowRule")
            .withRuleFactory(DaskWindowRule::new).toRule(DaskWindowRule.class);

    DaskWindowRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalWindow window = (LogicalWindow) rel;
        final RelTraitSet traitSet = window.getTraitSet().replace(out);
        RelNode transformedInput = convert(window.getInput(),
                window.getInput().getTraitSet().replace(DaskRel.CONVENTION));

        return DaskWindow.create(window.getCluster(), traitSet, transformedInput, window.getConstants(),
                window.getRowType(), window.groups);
    }
}
