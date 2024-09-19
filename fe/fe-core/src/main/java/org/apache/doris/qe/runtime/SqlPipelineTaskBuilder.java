package org.apache.doris.qe.runtime;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.distribute.worker.BackendWorker;
import org.apache.doris.nereids.trees.plans.distribute.worker.DistributedPlanWorker;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.protocol.TFastSerializer;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineFragmentParamsList;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.thrift.protocol.TCompactProtocol.Factory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class SqlPipelineTaskBuilder {
    public static SqlPipelineTask build(CoordinatorContext coordinatorContext,
            Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragmentsParam) {
        return new SqlPipelineTask(
                coordinatorContext,
                buildMultiFragmentTasks(coordinatorContext, workerToFragmentsParam)
        );
    }

    private static Map<Long, MultiFragmentsPipelineTask> buildMultiFragmentTasks(
            CoordinatorContext coordinatorContext, Map<DistributedPlanWorker, TPipelineFragmentParamsList> workerToFragmentsParam) {

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
                            coordinatorContext.queryId,
                            backend,
                            fragmentParamsList,
                            serializeFragments,
                            buildSingleFragmentPipelineTask(backend, fragmentParamsList)
                    )
            );
        }
        return fragmentTasks;
    }

    private static Map<Integer, SingleFragmentPipelineTask> buildSingleFragmentPipelineTask(
            Backend backend, TPipelineFragmentParamsList fragmentParamsList) {
        Map<Integer, SingleFragmentPipelineTask> tasks = Maps.newLinkedHashMap();
        for (TPipelineFragmentParams fragmentParams : fragmentParamsList.getParamsList()) {
            int fragmentId = fragmentParams.getFragmentId();
            tasks.put(fragmentId, new SingleFragmentPipelineTask(backend, fragmentId));
        }
        return tasks;
    }

    private static Map<DistributedPlanWorker, ByteString> serializeFragments(
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
