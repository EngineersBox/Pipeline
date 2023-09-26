package com.engineersbox.pipeline;

@FunctionalInterface
public interface InvocableStage<T, R> {

    StageResult<R> invoke(final T t);

}
