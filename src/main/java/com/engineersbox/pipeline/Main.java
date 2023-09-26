package com.engineersbox.pipeline;

public class Main {

    public static void main(final String[] args) {

    }

    // EXAMPLE
/*
    private Pipeline.Builder<RichIterable<Metric>> createPipeline(final LuaHandlerExtension handlerExtension) {
        final Pipeline.Builder<RichIterable<Metric>> pipelineBuilder = new Pipeline.Builder<>();
        if (handlerExtension != null) {
            pipelineBuilder.withStages(
//                    new HandlerSaturationPipelineStage(),
                    new PreProcessFilterPipelineStage(
                            handlerExtension::getPreProcessHandleDefinition,
                            this.contextBuilder
                    ),
                    new PipelineStage<Metric, EventBatch>("Parse metrics events") {
                        @Override
                        public StageResult<EventBatch> invoke(final Metric metric) {
                            final Proto.Event[] result = MetricProcessingTask.this.transformer.parseCoerceMetricEvents(
                                    MetricProcessingTask.this.retriever.lookup(metric),
                                    metric.getStructure(),
                                    metric
                            ).toArray(Proto.Event[]::new);
                            return new StageResult<>(
                                    StageResult.Type.COMBINE,
                                    (int) super.getContextAttribute(PreProcessFilterPipelineStage.FILTERED_COUNT_ATTRIBUTE),
                                    new EventBatch(
                                            result,
                                            metric.getExtensions()
                                    ),
                                    false
                            );
                        }
                    },
                    new AdapterProcessPipelineStage(
                            handlerExtension::getAdapterHandleDefinition,
                            this.contextBuilder,
                            this.eventTemplate
                    ),
                    new PostProcessFilterPipelineStage(
                            handlerExtension::getPostProcessHandleDefinition,
                            this.contextBuilder
                    )
            );
        } else {
            pipelineBuilder.withStage(new PipelineStage<Metric, Proto.Event[]>("Parse metrics events") {
                @Override
                public StageResult<Proto.Event[]> invoke(final Metric metric) {
                    final Proto.Event[] result = MetricProcessingTask.this.transformer.parseCoerceMetricEvents(
                            MetricProcessingTask.this.retriever.lookup(metric),
                            metric.getStructure(),
                            metric
                    ).toArray(Proto.Event[]::new);
                    return new StageResult<>(
                            StageResult.Type.COMBINE,
                            (int) super.getContextAttribute(PreProcessFilterPipelineStage.FILTERED_COUNT_ATTRIBUTE),
                            result,
                            false
                    );
                }
            });
        }
        return pipelineBuilder.withStages(
                new TerminatingPipelineStage<Proto.Event[]>("Send Riemann events") {
                    @Override
                    public void accept(final Proto.Event[] events) {
                        final IRiemannClient riemannClient = (IRiemannClient) super.getContextAttribute(RIEMANN_CLIENT_CTX_ATTRIBUTE);
                        try {
                            LOGGER.info(
                                    "Sending events: {}",
                                    Arrays.stream(events)
                                            .map((final Proto.Event event) -> String.format(
                                                    "%n - [Host: %s] [Description: %s] [Service: %s] [State: '%s'] [Float: %f] [Double: %f] [Int: %d] [Time: %d] [TTL: %f] [Tags: %s] [Attributes: %s]",
                                                    event.getHost(),
                                                    event.getDescription(),
                                                    event.getService(),
                                                    event.getState(),
                                                    event.getMetricF(),
                                                    event.getMetricD(),
                                                    event.getMetricSint64(),
                                                    event.getTimeMicros(),
                                                    event.getTtl(),
                                                    String.join(", ", event.getTagsList()),
                                                    event.getAttributesList()
                                                            .stream()
                                                            .map((final Proto.Attribute attr) -> String.format(
                                                                    "{ key: \"%s\", value: \"%s\" }",
                                                                    attr.getKey(),
                                                                    attr.getValue()
                                                            ))
                                                            .collect(Collectors.joining(", "))
                                            )).collect(Collectors.joining())
                            );
                            riemannClient.sendEvents(events).deref(1, TimeUnit.SECONDS);
                        } catch (final IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
        );
    } */

}
