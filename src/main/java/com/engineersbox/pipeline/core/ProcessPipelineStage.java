package com.engineersbox.pipeline.core;

import com.engineersbox.pipeline.PipelineStage;
import com.engineersbox.pipeline.StageResult;

import java.util.Collection;
import java.util.function.Function;

public abstract class ProcessPipelineStage<T, R> extends PipelineStage<Collection<T>, Collection<R>> implements Function<T, R> {


    public ProcessPipelineStage(final String name) {
        super(name);
    }

    @Override
    public abstract R apply(final T element);

    @Override
    public StageResult<Collection<R>> invoke(final Collection<T> previousResult) {
        final Collection<R> result = previousResult.stream()
                .map(this)
                .toList();
        return new StageResult<>(
                StageResult.Type.SINGLETON,
                result,
                false
        );
    }
}
