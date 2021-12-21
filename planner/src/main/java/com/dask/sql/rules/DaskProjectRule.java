package com.dask.sql.rules;

import com.dask.sql.nodes.DaskRel;
import com.dask.sql.nodes.DaskProject;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

public class DaskProjectRule extends ConverterRule {
    public static final DaskProjectRule INSTANCE = Config.INSTANCE.withConversion(LogicalProject.class,
            p -> !p.containsOver(), Convention.NONE, DaskRel.CONVENTION, "DaskProjectRule")
            .withRuleFactory(DaskProjectRule::new).toRule(DaskProjectRule.class);

    DaskProjectRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalProject project = (LogicalProject) rel;
        final RelTraitSet traitSet = project.getTraitSet().replace(out);
        RelNode transformedInput = convert(project.getInput(),
                project.getInput().getTraitSet().replace(DaskRel.CONVENTION));

        return DaskProject.create(project.getCluster(), traitSet, transformedInput, project.getProjects(),
                project.getRowType());
    }
}
