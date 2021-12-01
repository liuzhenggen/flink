package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.function.Function;

/** Sink function for converting upserts into Elasticsearch {@link ActionRequest}s. */
@Internal
class RowElasticsearch7SinkFunction extends RowElasticsearchSinkFunction {

    private final IndexGenerator indexGenerator;
    private final String docType;
    private final SerializationSchema<RowData> serializationSchema;
    private final XContentType contentType;
    private final RequestFactory requestFactory;
    private final Function<RowData, String> createKey;
    private final Function<RowData, String> createRouting;

    public RowElasticsearch7SinkFunction(
            IndexGenerator indexGenerator,
            @Nullable String docType, // this is deprecated in es 7+
            SerializationSchema<RowData> serializationSchema,
            XContentType contentType,
            RequestFactory requestFactory,
            Function<RowData, String> createKey,
            Function<RowData, String> createRouting) {
        super(indexGenerator, docType, serializationSchema, contentType, requestFactory, createKey);
        this.indexGenerator = Preconditions.checkNotNull(indexGenerator);
        this.docType = docType;
        this.serializationSchema = Preconditions.checkNotNull(serializationSchema);
        this.contentType = Preconditions.checkNotNull(contentType);
        this.requestFactory = Preconditions.checkNotNull(requestFactory);
        this.createKey = Preconditions.checkNotNull(createKey);
        this.createRouting = Preconditions.checkNotNull(createRouting);
    }

    @Override
    public void process(RowData element, RuntimeContext ctx, RequestIndexer indexer) {
        switch (element.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                processUpsert(element, indexer);
                break;
            case UPDATE_BEFORE:
            case DELETE:
                processDelete(element, indexer);
                break;
            default:
                throw new TableException("Unsupported message kind: " + element.getRowKind());
        }
    }

    private void processUpsert(RowData row, RequestIndexer indexer) {
        final byte[] document = serializationSchema.serialize(row);
        final String key = createKey.apply(row);
        final String routing = createRouting.apply(row);
        if (key != null) {
            final UpdateRequest updateRequest =
                    requestFactory.createUpdateRequest(
                            indexGenerator.generate(row), docType, key, contentType, document);
            updateRequest.routing(routing);
            indexer.add(updateRequest);
        } else {
            final IndexRequest indexRequest =
                    requestFactory.createIndexRequest(
                            indexGenerator.generate(row), docType, key, contentType, document);
            indexRequest.routing(routing);
            indexer.add(indexRequest);
        }
    }

    private void processDelete(RowData row, RequestIndexer indexer) {
        final String key = createKey.apply(row);
        final String routing = createRouting.apply(row);
        final DeleteRequest deleteRequest =
                requestFactory.createDeleteRequest(indexGenerator.generate(row), docType, key);
        deleteRequest.routing(routing);
        indexer.add(deleteRequest);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowElasticsearch7SinkFunction that = (RowElasticsearch7SinkFunction) o;
        return Objects.equals(indexGenerator, that.indexGenerator)
                && Objects.equals(docType, that.docType)
                && Objects.equals(serializationSchema, that.serializationSchema)
                && contentType == that.contentType
                && Objects.equals(requestFactory, that.requestFactory)
                && Objects.equals(createKey, that.createKey)
                && Objects.equals(createRouting, that.createRouting);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                indexGenerator,
                docType,
                serializationSchema,
                contentType,
                requestFactory,
                createKey,
                createRouting);
    }
}
