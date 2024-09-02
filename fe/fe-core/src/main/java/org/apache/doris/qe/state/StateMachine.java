// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.qe.state;

import org.eclipse.jetty.util.BlockingArrayQueue;

import java.util.List;
import java.util.Queue;
import java.util.Vector;

/** StateMachine */
public class StateMachine<C, E> {
    private volatile boolean stop = false;
    private volatile C context;
    private List<StateListener<C, E>> eventListeners;
    private Queue<E> eventQueue;

    public StateMachine(C context) {
        this.context = context;
        this.eventListeners = new Vector<>();
        this.eventQueue = new BlockingArrayQueue<>();
    }

    public void stop() {
        this.stop = true;
    }

    public void startEventLoop() {
        while (!stop && !eventQueue.isEmpty()) {
            E event = eventQueue.poll();
            beforeChange(context, event);
            for (StateListener<C, E> eventListener : eventListeners) {
                if (eventListener.canApply(context, event)) {
                    eventListener.onEvent(context, event, this);
                    break;
                }
            }
            afterChange(context, event);
        }
    }

    public synchronized C getContext() {
        return context;
    }

    public void eventSwitch(C context, E event) {

    }

    // public StateListenerFluentBuilder<C, E> whenState(Predicate<C> statePredicate) {
    //     StateListenerFluentBuilder<S, E> builder = new StateListenerFluentBuilder<>(this);
    //     builder.whenState(statePredicate);
    //     return builder;
    // }
    //
    // public StateListenerFluentBuilder<S, E> whenEvent(Predicate<E> eventPredicate) {
    //     StateListenerFluentBuilder<S, E> builder = new StateListenerFluentBuilder<>(this);
    //     builder.whenEvent(eventPredicate);
    //     return builder;
    // }

    public void triggerEvent(E event) {
        eventQueue.add(event);
    }

    public void beforeChange(C context, E event) {

    }

    public void afterChange(C context, E event) {

    }


    enum QueryEvent {
        START, CANCEL
    }

    enum QueryState {
        INIT, BUILT_RPC_PARAM, SERIALIZED_RPC_PARAM, SENT_RPC, SUCCEED, CANCELED,
    }

    static class QueryContext {
        public volatile QueryState state = QueryState.INIT;
    }

    public static void main(String[] args) {
        // Deadline deadline = Deadline.ofFutureMillis(5000000);

        // QueryContext context = new QueryContext();

        // StateMachine<QueryContext, QueryEvent> stateMachine = new StateMachine<QueryContext, QueryEvent>(context) {
        //     @Override
        //     public void beforeChange(QueryContext context, QueryEvent event) {
        //         if (context.state != QueryState.CANCELED) {
        //             deadline.check(remain -> {
        //                 if (remain <= 0) {
        //                     triggerEvent(QueryEvent.CANCEL);
        //                 }
        //             });
        //         }
        //     }
        //
        //     @Override
        //     public void eventSwitch(QueryContext context, QueryEvent event) {
        //         switch (context.state) {
        //             case INIT:
        //                 if (event == QueryEvent.START) {
        //
        //                 }
        //             case BUILT_RPC_PARAM:
        //             case SERIALIZED_RPC_PARAM:
        //             case SENT_RPC:
        //             case SUCCEED:
        //             case CANCELED:
        //         }
        //         throw new IllegalStateException("Can not process this state: " + context.state);
        //     }
        // };

        // stateMachine
        //     .whenState(ctx -> ctx.state == QueryState.INIT)
        //     .whenEvent(event -> event == QueryEvent.BUILD_RPC_PARAM)
        //     .then((ctx, event, sm) -> {
        //         ctx.state = QueryState.BUILT_RPC_PARAM;
        //         sm.triggerEvent(QueryEvent.SERIALIZE_RPC_PARAM);
        //     });
        //
        // stateMachine
        //     .whenState(ctx -> ctx.state == QueryState.BUILT_RPC_PARAM)
        //     .whenEvent(event -> event == QueryEvent.SERIALIZE_RPC_PARAM)
        //     .then((ctx, event, sm) -> {
        //         ctx.state = QueryState.SERIALIZED_RPC_PARAM;
        //         sm.triggerEvent(QueryEvent.SEND_RPC);
        //     });
        //
        // stateMachine
        //     .whenState(ctx -> ctx.state == QueryState.SERIALIZED_RPC_PARAM)
        //     .whenEvent(event -> event == QueryEvent.SEND_RPC)
        //     .then((ctx, event, sm) -> {
        //         ctx.state = QueryState.SENT_RPC;
        //         sm.triggerEvent(QueryEvent.WAIT_RPC_RESPONSE);
        //     });
        //
        // stateMachine
        //     .whenState(ctx -> ctx.state == QueryState.SENT_RPC)
        //     .whenEvent(event -> event == QueryEvent.WAIT_RPC_RESPONSE)
        //     .then((ctx, event, sm) -> {
        //         ctx.state = QueryState.SUCCEED;
        //         sm.triggerEvent(QueryEvent.SUCCESS);
        //     });
        //
        // stateMachine.triggerEvent(QueryEvent.BUILD_RPC_PARAM);
        // stateMachine.startEventLoop();

        // stateMachine.when(QueryEvent.BUILD_RPC_PARAM, (ctx, event, sm) -> {
        //     if (ctx.state == QueryState.INIT) {
        //         sm.triggerEvent(QueryEvent.SERIALIZE_RPC_PARAM);
        //     }
        // });
        //
        // stateMachine.when(QueryEvent.BUILD_RPC_PARAM, (ctx, event, sm) -> {
        //     sm.triggerEvent(QueryEvent.SERIALIZE_RPC_PARAM);
        // });
        //
        // stateMachine.when(QueryEvent.SERIALIZE_RPC_PARAM, (ctx, event, sm) -> {
        //     sm.triggerEvent(QueryEvent.WAIT_RPC_RESPONSE);
        // });
        //
        // stateMachine.when(QueryEvent.SEND_RPC, (ctx, event, sm) -> {
        //     sm.triggerEvent(QueryEvent.WAIT_RPC_RESPONSE);
        // });
        //
        // stateMachine.when(QueryEvent.WAIT_RPC_RESPONSE, (ctx, event, sm) -> {
        //     Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);
        //     sm.triggerEvent(QueryEvent.SUCCEED);
        // });
        //
        // stateMachine.triggerEvent(QueryEvent.BUILD_RPC_PARAM);
    }

    // public static class StateListenerFluentBuilder<S, E> {
    //     private final StateMachine<S, E> stateMachine;
    //     private final List<BiPredicate<S, E>> predicates;
    //
    //     public StateListenerFluentBuilder(StateMachine<S, E> stateMachine) {
    //         this.stateMachine = stateMachine;
    //         this.predicates = new ArrayList<>();
    //     }
    //
    //     public StateListenerFluentBuilder<S, E> whenState(Predicate<S> condition) {
    //         predicates.add((state, event) -> condition.test(state));
    //         return this;
    //     }
    //
    //     public StateListenerFluentBuilder<S, E> whenEvent(Predicate<E> condition) {
    //         predicates.add((state, event) -> condition.test(event));
    //         return this;
    //     }
    //
    //     public void then(StateListener<S, E> listener) {
    //         synchronized (stateMachine) {
    //             stateMachine.eventListeners.add(new StateListener<S, E>() {
    //                 @Override
    //                 public boolean canApply(S state, E event) {
    //                     for (BiPredicate<S, E> predicate : predicates) {
    //                         if (!predicate.test(state, event)) {
    //                             return false;
    //                         }
    //                     }
    //                     return listener.canApply(state, event);
    //                 }
    //
    //                 @Override
    //                 public void onEvent(S state, E event, StateMachine<S, E> stateMachine) {
    //                     listener.onEvent(state, event, stateMachine);
    //                 }
    //             });
    //         }
    //     }
    // }
    //
    // public static class StateListenerCondition<S, E> {
    //     private final StateMachine<S, E> stateMachine;
    //     private final BiPredicate<S, E> condition;
    //
    //     public StateListenerCondition(StateMachine<S, E> stateMachine, BiPredicate<S, E> condition) {
    //         this.stateMachine = stateMachine;
    //         this.condition = condition;
    //     }
    //
    //     public StateMachine<S, E> then(StateListener<S, E> listener) {
    //         stateMachine.eventListeners.add(new StateListener<S, E>() {
    //             @Override
    //             public boolean canApply(S state, E event) {
    //                 return condition.test(state, event) && listener.canApply(state, event);
    //             }
    //
    //             @Override
    //             public void onEvent(S state, E event, StateMachine<S, E> stateMachine) {
    //                 listener.onEvent(state, event, stateMachine);
    //             }
    //         });
    //         return stateMachine;
    //     }
    // }
}
