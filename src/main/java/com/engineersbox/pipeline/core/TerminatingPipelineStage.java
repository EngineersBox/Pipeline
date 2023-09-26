package com.engineersbox.pipeline.core;

import com.engineersbox.pipeline.PipelineStage;
import com.engineersbox.pipeline.StageResult;

import java.util.function.Consumer;

public abstract class TerminatingPipelineStage<T> extends PipelineStage<T, Void> implements Consumer<T> {


    public TerminatingPipelineStage(final String name) {
        super(name);
    }

    @Override
    public abstract void accept(T t);

    @Override
    public StageResult<Void> invoke(final T previousResult) {
        this.accept(previousResult);
        return new StageResult<>(
                StageResult.Type.SINGLETON,
                null,
                true
        );
    }
}
