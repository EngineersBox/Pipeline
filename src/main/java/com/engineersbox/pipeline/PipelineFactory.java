package com.engineersbox.pipeline;

public class PipelineFactory {

    public static <T> Pipeline<T> defaultPipeline() {
        final Pipeline<T> pipeline = new Pipeline.Builder<T>()
                .withStages()
                .build();
        return pipeline;
    }

}
