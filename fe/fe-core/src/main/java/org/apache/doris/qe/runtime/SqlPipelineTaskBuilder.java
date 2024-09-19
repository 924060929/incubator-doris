package org.apache.doris.qe.runtime;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.ExecContext;
import org.apache.doris.qe.protocol.TFastSerializer;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.protocol.TCompactProtocol.Factory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

public class SqlPipelineTaskBuilder {
    private static final Logger LOG = LogManager.getLogger(SqlPipelineTaskBuilder.class);

    private final ExecContext execContext;
    private final CoordinatorContext coordinatorContext;

    private SqlPipelineTaskBuilder(ExecContext execContext, CoordinatorContext coordinatorContext) {
        this.execContext = Objects.requireNonNull(execContext, "execContext can not be null");
        this.coordinatorContext = Objects.requireNonNull(coordinatorContext, "coordinatorContext can not be null");
    }

    public static SqlPipelineTask build(ExecContext execContext, CoordinatorContext coordinatorContext,
            Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragmentsParam) {
        SqlPipelineTaskBuilder builder = new SqlPipelineTaskBuilder(execContext, coordinatorContext);
        return builder.buildTask(execContext, coordinatorContext, workerToFragmentsParam);
    }

    private SqlPipelineTask buildTask(ExecContext execContext, CoordinatorContext coordinatorContext,
            Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragmentsParam) {
        return new SqlPipelineTask(
                execContext,
                coordinatorContext,
                buildMultiFragmentTasks(execContext, workerToFragmentsParam)
        );
    }

    private Map<Long, MultiFragmentsPipelineTask> buildMultiFragmentTasks(
            ExecContext execContext, Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragmentsParam) {

        Map<DistributedPlanWorker, ByteString> workerToSerializeFragments = serializeFragments(workerToFragmentsParam);

        Map<Long, MultiFragmentsPipelineTask> fragmentTasks = Maps.newLinkedHashMap();
        for (Entry<DistributedPlanWorker, TPipelineFragmentParamsList> kv :
                workerToFragmentsParam.entrySet()) {
            BackendWorker worker = (BackendWorker) kv.getKey();
            TPipelineFragmentParamsList fragmentParamsList = kv.getValue();
            ByteString serializeFragments = workerToSerializeFragments.get(worker);

            Backend backend = worker.getBackend();
            fragmentTasks.put(
                    worker.id(),
                    new MultiFragmentsPipelineTask(
                            execContext.queryId,
                            backend,
                            fragmentParamsList,
                            serializeFragments,
                            buildSingleFragmentPipelineTask(backend, fragmentParamsList)
                    )
            );
        }
        return fragmentTasks;
    }

    private Map<Integer, SingleFragmentPipelineTask> buildSingleFragmentPipelineTask(
            Backend backend, TPipelineFragmentParamsList fragmentParamsList) {
        Map<Integer, SingleFragmentPipelineTask> tasks = Maps.newLinkedHashMap();
        for (TPipelineFragmentParams fragmentParams : fragmentParamsList.getParamsList()) {
            int fragmentId = fragmentParams.getFragmentId();
            tasks.put(fragmentId, new SingleFragmentPipelineTask(backend, fragmentId));
        }
        return tasks;
    }

    private Map<DistributedPlanWorker, ByteString> serializeFragments(
            Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragmentsParam){
        return workerToFragmentsParam.entrySet()
                .parallelStream()
                .map(kv -> {
                    try {
                        // zero copy
                        ByteString serializeString =
                                new TFastSerializer(1024, new Factory()).serialize(kv.getValue());
                        return Pair.of(kv.getKey(), serializeString);
                    } catch (Throwable t) {
                        throw new IllegalStateException(t.getMessage(), t);
                    }
                })
                .collect(Collectors.toMap(Pair::key, Pair::value));
    }
}
