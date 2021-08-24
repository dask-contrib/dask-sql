package com.dask.sql.application;

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

public class DaskProgram {
    private final Program beforeOptimizationProgram;
    private final Program optimizationProgram;

    public DaskProgram(RelOptPlanner planner) {
        final DecorrelateProgram decorrelateProgram = new DecorrelateProgram();
        final TrimFieldsProgram trimProgram = new TrimFieldsProgram();
        final PreOptimizationProgram preOptimizeProgram = new PreOptimizationProgram();
        final ConvertProgram convertProgram = new ConvertProgram(planner);
        final OptimizeProgram volcanoProgram = new OptimizeProgram(planner);

        this.beforeOptimizationProgram = Programs.sequence(decorrelateProgram, trimProgram, preOptimizeProgram);
        this.optimizationProgram = Programs.sequence(convertProgram, volcanoProgram);
    }

    public RelNode runUntilOptimization(RelNode rel) {
        return run(this.beforeOptimizationProgram, rel);
    }

    public RelNode runOptimization(RelNode rel) {
        return run(this.optimizationProgram, rel);
    }

    private RelNode run(Program program, RelNode rel) {
        final RelTraitSet desiredTraits = rel.getTraitSet().replace(DaskRel.CONVENTION).simplify();
        return program.run(null, rel, desiredTraits, List.of(), List.of());
    }

    private static interface DaskProgramWrapper extends Program {
        public RelNode run(RelNode rel, RelTraitSet relTraitSet);

        @Override
        public default RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits,
                List<RelOptMaterialization> materializations, List<RelOptLattice> lattices) {
            return run(rel, requiredOutputTraits);
        }
    }

    private static class DecorrelateProgram implements DaskProgramWrapper {
        @Override
        public RelNode run(RelNode rel, RelTraitSet relTraitSet) {
            final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
            return RelDecorrelator.decorrelateQuery(rel, relBuilder);
        }
    }

    private static class TrimFieldsProgram implements DaskProgramWrapper {
        public RelNode run(RelNode rel, RelTraitSet relTraitSet) {
            final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
            return new RelFieldTrimmer(null, relBuilder).trim(rel);
        }
    }

    private static class PreOptimizationProgram implements Program {
        @Override
        public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits,
                List<RelOptMaterialization> materializations, List<RelOptLattice> lattices) {
            List<RelOptRule> rules = List.of(CoreRules.AGGREGATE_PROJECT_MERGE,
                    CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS, CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS,
                    CoreRules.AGGREGATE_REDUCE_FUNCTIONS, CoreRules.AGGREGATE_MERGE,
                    CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN, CoreRules.AGGREGATE_JOIN_REMOVE,
                    CoreRules.FILTER_AGGREGATE_TRANSPOSE, CoreRules.JOIN_CONDITION_PUSH, CoreRules.FILTER_INTO_JOIN,
                    CoreRules.PROJECT_MERGE, CoreRules.FILTER_MERGE, CoreRules.PROJECT_JOIN_TRANSPOSE,
                    CoreRules.PROJECT_REMOVE, CoreRules.PROJECT_REDUCE_EXPRESSIONS, CoreRules.FILTER_REDUCE_EXPRESSIONS,
                    CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM, CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW);
            Program preOptimizeProgram = Programs.hep(rules, true, DefaultRelMetadataProvider.INSTANCE);
            return preOptimizeProgram.run(planner, rel, requiredOutputTraits, materializations, lattices);
        }
    }

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

    private static class OptimizeProgram implements DaskProgramWrapper {
        private RelOptPlanner planner;

        public OptimizeProgram(RelOptPlanner planner) {
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
