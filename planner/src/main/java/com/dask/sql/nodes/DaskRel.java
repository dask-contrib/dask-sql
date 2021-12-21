package com.dask.sql.nodes;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

public interface DaskRel extends RelNode {
    Convention CONVENTION = DaskConvention.INSTANCE;
}
