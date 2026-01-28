package org.apache.doris.planner;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPlanNode;

import java.util.Collections;

public class LocalExchangeNode extends PlanNode {
    public static final String EXCHANGE_NODE = "LOCAL-EXCHANGE";

    private TPartitionType partitionType;

    /**
     * use for Nereids only.
     */
    public LocalExchangeNode(PlanNodeId id, PlanNode inputNode) {
        super(id, inputNode, EXCHANGE_NODE);
        this.offset = 0;
        this.limit = -1;
        this.conjuncts = Collections.emptyList();
        this.children.add(inputNode);
        TupleDescriptor outputTupleDesc = inputNode.getOutputTupleDesc();
    }

    @Override
    protected void toThrift(TPlanNode msg) {

    }
}
