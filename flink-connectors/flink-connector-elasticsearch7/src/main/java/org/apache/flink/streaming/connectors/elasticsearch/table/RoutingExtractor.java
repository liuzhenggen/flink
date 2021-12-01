package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** An extractor for a Elasticsearch key from a {@link RowData}. */
@Internal
class RoutingExtractor implements Function<RowData, String>, Serializable {
    private final FieldFormatter fieldFormatter;

    private interface FieldFormatter extends Serializable {
        String format(RowData rowData);
    }

    private RoutingExtractor(FieldFormatter fieldFormatter) {
        this.fieldFormatter = fieldFormatter;
    }

    @Override
    public String apply(RowData rowData) {
        return fieldFormatter.format(rowData);
    }

    private static class ColumnWithIndex {
        public TableColumn column;
        public int index;

        public ColumnWithIndex(TableColumn column, int index) {
            this.column = column;
            this.index = index;
        }

        public LogicalType getType() {
            return column.getType().getLogicalType();
        }

        public int getIndex() {
            return index;
        }
    }

    public static Function<RowData, String> createRoutingExtractor(
            TableSchema schema, String routingField) {
        do {
            if (routingField == null) {
                break;
            }

            Map<String, ColumnWithIndex> namesToColumns = new HashMap<>();
            List<TableColumn> tableColumns = schema.getTableColumns();
            for (int i = 0; i < schema.getFieldCount(); i++) {
                TableColumn column = tableColumns.get(i);
                namesToColumns.put(column.getName(), new ColumnWithIndex(column, i));
            }
            ColumnWithIndex routingColumn = namesToColumns.get(routingField);
            if (routingColumn == null) {
                break;
            }

            return new RoutingExtractor(toFormatter(routingColumn.index, routingColumn.getType()));
        } while (false);

        return (Function<RowData, String> & Serializable) (row) -> null;
    }

    private static FieldFormatter toFormatter(int index, LogicalType type) {
        switch (type.getTypeRoot()) {
            case DATE:
                return (row) -> LocalDate.ofEpochDay(row.getInt(index)).toString();
            case TIME_WITHOUT_TIME_ZONE:
                return (row) ->
                        LocalTime.ofNanoOfDay((long) row.getInt(index) * 1_000_000L).toString();
            case INTERVAL_YEAR_MONTH:
                return (row) -> Period.ofDays(row.getInt(index)).toString();
            case INTERVAL_DAY_TIME:
                return (row) -> Duration.ofMillis(row.getLong(index)).toString();
            case DISTINCT_TYPE:
                return toFormatter(index, ((DistinctType) type).getSourceType());
            default:
                RowData.FieldGetter fieldGetter = RowData.createFieldGetter(type, index);
                return (row) -> fieldGetter.getFieldOrNull(row).toString();
        }
    }
}
