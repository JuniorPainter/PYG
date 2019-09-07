package cn.wi.canal;


import cn.wi.util.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author xianlawei
 */
public class CanalClient {
    public static void main(String[] args) {
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(
                        //canal在那台机器上
                        "192.168.72.132",
                        11111),
                "example",
                "root",
                "123456");
        int batchSize = 1;
        int emptyCount = 1;
        try {
            connector.connect();
            //订阅所有的库、所有的表
            connector.subscribe(".*\\..*");
            connector.rollback();
            int totalCount = 100;
            while (totalCount > emptyCount) {
                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();

                if (batchId != -1L || size != 0) {
                    System.out.println("<<<<<<<<<<<<<<<<<<<<<");
                    analysis(message.getEntries(), emptyCount);
                    ++emptyCount;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connector.disconnect();
        }
    }

    private static void analysis(List<Entry> entries, int emptyCount) {
        for (Entry entry : entries) {
            if (entry.getEntryType() == EntryType.TRANSACTIONBEGIN) {
                continue;
            }
            if (entry.getEntryType() == EntryType.TRANSACTIONEND) {
                continue;
            }
            RowChange rowChange = null;
            try {
                rowChange = RowChange.parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }

            //获取关键字段
            assert rowChange != null;
            final EventType eventType = rowChange.getEventType();
            final String logfileName = entry.getHeader().getLogfileName();
            final long logfileOffset = entry.getHeader().getLogfileOffset();
            final String dbname = entry.getHeader().getSchemaName();
            final String tableName = entry.getHeader().getTableName();
            //执行时间
            long timestamp = entry.getHeader().getExecuteTime();

            for (RowData rowData : rowChange.getRowDatasList()) {

                System.out.println("<<<<<<<<<<<<<logfileName:" + logfileName + ",logfileOffset:" + logfileOffset
                        + ",dbname:" + dbname + ",rowData:" + rowData.toString());
                if (eventType == EventType.DELETE) {
                    dataDetails(rowData.getBeforeColumnsList(), logfileName, logfileOffset, dbname, tableName, eventType, emptyCount, timestamp);
                } else if (eventType == EventType.INSERT) {
                    dataDetails(rowData.getAfterColumnsList(), logfileName, logfileOffset, dbname, tableName, eventType, emptyCount, timestamp);
                } else {
                    dataDetails(rowData.getAfterColumnsList(), logfileName, logfileOffset, dbname, tableName, eventType, emptyCount, timestamp);
                }
            }
        }

    }

    private static void dataDetails(List<Column> columns, String logfileName, long logfileOffset, String dbname, String tableName, EventType eventType, int emptyCount, long timestamp) {

        // 找到当前那些列发生了改变  以及改变的值
        List<ColumnValuePair> columnValueList = new ArrayList<ColumnValuePair>();

        for (Column column : columns) {
            ColumnValuePair columnValuePair = new ColumnValuePair(column.getName(), column.getValue(), column.getUpdated());
            columnValueList.add(columnValuePair);
        }

        String data = logfileName + "##" + logfileOffset + "##" + dbname + "##" + tableName + "##" + eventType + "##" + columnValueList + "##" + emptyCount;

        String sendKey = UUID.randomUUID().toString();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("logFileName", logfileName);
        jsonObject.put("logFileOffset", logfileOffset);
        jsonObject.put("dbName", dbname);
        jsonObject.put("tableName", tableName);
        jsonObject.put("eventType", eventType);
        jsonObject.put("columnValueList", columnValueList);
        jsonObject.put("emptyCount", emptyCount);
        jsonObject.put("timestamp", timestamp);

        // 拼接所有binlog解析的字段
        String jsonStr = JSON.toJSONString(jsonObject);

        System.out.println(jsonStr);
        KafkaUtil.sendData("canal", sendKey, jsonStr);
    }

    static class ColumnValuePair {
        private String columnName;
        private String columnValue;
        private Boolean isValid;

        ColumnValuePair(String columnName, String columnValue, Boolean isValid) {
            this.columnName = columnName;
            this.columnValue = columnValue;
            this.isValid = isValid;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnValue() {
            return columnValue;
        }

        public void setColumnValue(String columnValue) {
            this.columnValue = columnValue;
        }

        public Boolean getIsValid() {
            return isValid;
        }

        public void setIsValid(Boolean isValid) {
            this.isValid = isValid;
        }
    }

}
