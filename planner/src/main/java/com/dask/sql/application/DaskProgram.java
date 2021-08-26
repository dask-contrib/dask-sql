package com.dask.sql.application;

import java.util.Arrays;
import java.util.List;

import com.dask.sql.nodes.DaskRel;

import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

/**
 * DaskProgram is the optimization program which is executed on a tree of
 * relational algebras.
 *
 * It consists of five steps: decorrelation, trimming (removing unneeded
 * fields), executing a fixed set of rules, converting into the correct trait
 * (which means in our case: from logical to dask) and finally a cost-based
 * optimization.
 */
public class DaskProgram {
    private final Program mainProgram;

    public DaskProgram(RelOptPlanner planner) {
        final DecorrelateProgram decorrelateProgram = new DecorrelateProgram();
        final TrimFieldsProgram trimProgram = new TrimFieldsProgram();
        final FixedRulesProgram fixedRulesProgram = new FixedRulesProgram();
        final ConvertProgram convertProgram = new ConvertProgram(planner);
        final CostBasedOptimizationProgram costBasedOptimizationProgram = new CostBasedOptimizationProgram(planner);

        this.mainProgram = Programs.sequence(decorrelateProgram, trimProgram, fixedRulesProgram, convertProgram,
                costBasedOptimizationProgram);
    }

    public RelNode run(RelNode rel) {
        final RelTraitSet desiredTraits = rel.getTraitSet().replace(DaskRel.CONVENTION).simplify();
        return this.mainProgram.run(null, rel, desiredTraits, Arrays.asList(), Arrays.asList());
    }

    /**
     * DaskProgramWrapper is a helper for auto-filling unneeded arguments in
     * Programs
     */
    private static interface DaskProgramWrapper extends Program {
        public RelNode run(RelNode rel, RelTraitSet relTraitSet);

        @Override
        public default RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits,
                List<RelOptMaterialization> materializations, List<RelOptLattice> lattices) {
            return run(rel, requiredOutputTraits);
        }
    }

    /**
     * DecorrelateProgram decorrelates a query, by tunring them into e.g. JOINs
     */
    private static class DecorrelateProgram implements DaskProgramWrapper {
        @Override
        public RelNode run(RelNode rel, RelTraitSet relTraitSet) {
            final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
            return RelDecorrelator.decorrelateQuery(rel, relBuilder);
        }
    }

    /**
     * TrimFieldsProgram removes unneeded fields from the REL steps
     */
    private static class TrimFieldsProgram implements DaskProgramWrapper {
        public RelNode run(RelNode rel, RelTraitSet relTraitSet) {
            final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
            return new RelFieldTrimmer(null, relBuilder).trim(rel);
        }
    }

    /**
     * FixedRulesProgram applies a fixed set of conversion rules, which we always
     */
    private static class FixedRulesProgram implements Program {
        static private final List<RelOptRule> RULES = Arrays.asList(CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS,
                CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS, CoreRules.AGGREGATE_PROJECT_MERGE,
                CoreRules.AGGREGATE_REDUCE_FUNCTIONS, CoreRules.AGGREGATE_MERGE,
                CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN, CoreRules.AGGREGATE_JOIN_REMOVE,
                CoreRules.PROJECT_MERGE, CoreRules.FILTER_MERGE, CoreRules.PROJECT_REMOVE,
                CoreRules.PROJECT_REDUCE_EXPRESSIONS, CoreRules.FILTER_REDUCE_EXPRESSIONS,
                CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM, CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);

        @Override
        public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits,
                List<RelOptMaterialization> materializations, List<RelOptLattice> lattices) {

            Program fixedRulesProgram = Programs.hep(RULES, true, DefaultRelMetadataProvider.INSTANCE);
            return fixedRulesProgram.run(planner, rel, requiredOutputTraits, materializations, lattices);
        }
    }

    /**
     * ConvertProgram marks the rel as "to-be-converted-into-the-dask-trait" by the
     * upcoming volcano planner.
     */
    private static class ConvertProgram implements DaskProgramWrapper {
        private RelOptPlanner planner;

        public ConvertProgram(RelOptPlanner planner) {
            this.planner = planner;
        }

        @Override
        public RelNode run(final RelNode rel, final RelTraitSet requiredOutputTraits) {
            planner.setRoot(rel);

            if (rel.getTraitSet().equals(requiredOutputTraits)) {
                return rel;
            }
            final RelNode convertedRel = planner.changeTraits(rel, requiredOutputTraits);
            assert convertedRel != null;
            return convertedRel;
        }
    }

    /**
     * CostBasedOptimizationProgram applies a cost-based optimization, which can
     * take the size of the inputs into account (not implemented now).
     */
    private static class CostBasedOptimizationProgram implements DaskProgramWrapper {
        private RelOptPlanner planner;

        public CostBasedOptimizationProgram(RelOptPlanner planner) {
            this.planner = planner;
        }

        @Override
        public RelNode run(RelNode rel, RelTraitSet requiredOutputTraits) {
            planner.setRoot(rel);
            final RelOptPlanner planner2 = planner.chooseDelegate();
            final RelNode optimizedRel = planner2.findBestExp();
            assert optimizedRel != null : "could not implement exp";
            return optimizedRel;
        }
    }
}
