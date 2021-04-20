/*
package com.yahoo.maha.service.calcite.avatica;
import com.yahoo.maha.core.query.*;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.ArrayType;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.ColumnMetaData.ScalarType;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.Style;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.remote.Service.CloseConnectionResponse;
import org.apache.calcite.avatica.remote.Service.CloseStatementResponse;
import org.apache.calcite.avatica.remote.Service.ConnectionSyncResponse;
import org.apache.calcite.avatica.remote.Service.CreateStatementResponse;
import org.apache.calcite.avatica.remote.Service.DatabasePropertyResponse;
import org.apache.calcite.avatica.remote.Service.ErrorResponse;
import org.apache.calcite.avatica.remote.Service.ExecuteBatchResponse;
import org.apache.calcite.avatica.remote.Service.ExecuteResponse;
import org.apache.calcite.avatica.remote.Service.FetchResponse;
import org.apache.calcite.avatica.remote.Service.OpenConnectionResponse;
import org.apache.calcite.avatica.remote.Service.PrepareResponse;
import org.apache.calcite.avatica.remote.Service.Response;
import org.apache.calcite.avatica.remote.Service.ResultSetResponse;
import org.apache.calcite.avatica.remote.Service.RpcMetadataResponse;
import org.apache.calcite.avatica.remote.Service.SyncResultsResponse;


import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.*;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AvaticaHelper {
    private static ColumnMetaData getArrayColumnMetaData(ScalarType componentType, int index,
                                                         String name) {
        ArrayType arrayType = ColumnMetaData.array(componentType, "Array", Rep.ARRAY);
        return new ColumnMetaData(
                index, false, true, false, false, DatabaseMetaData.columnNullable,
                true, -1, name, name, null,
                0, 0, null, null, arrayType, true, false, false,
                "ARRAY");
    }
    public static String getHostName() {
        try {
           return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }

*/
/*
    public static ResultSetResponse getResultSetResponse(QueryRowList queryRowList, String connectionID, String sql, int statementID) {

        final RpcMetadataResponse rpcMetadata = new RpcMetadataResponse(getHostName());
        List<ColumnMetaData> columns = new ArrayList<>();

        columns.add(MetaImpl.columnMetaData("str", 0, String.class, true));
        List<AvaticaParameter> params = new ArrayList<>();
        params.add(new AvaticaParameter(false, 10, 0, Types.VARCHAR, "VARCHAR",
                String.class.getName(), "str"));

        Meta.CursorFactory cursorFactory = Meta.CursorFactory.create(Style.LIST, String.class,
                Arrays.asList("str"));
        // The row values
        List<Object> rows = new ArrayList<>();
        rows.add(new String[] {"str_value1"});
        rows.add(new String[] {"str_value2"});

        // Create the signature and frame using the metadata and values
        Signature signature = Signature.create(columns, sql, params, cursorFactory,
                Meta.StatementType.SELECT);
        Frame frame = Frame.create(0, true, rows);

        // And then create a ResultSetResponse
        return new ResultSetResponse(connectionID, statementID, false,
                signature, frame, -1, rpcMetadata);
    }

    public static List<Response> getResponses(String connectionID, String sql, int statementID) {
        final RpcMetadataResponse rpcMetadata = new RpcMetadataResponse("localhost:8765");
        LinkedList<Response> responses = new LinkedList<>();

        // Nested classes (Signature, ColumnMetaData, CursorFactory, etc) are implicitly getting tested)

        // Stub out the metadata for a row
        ScalarType arrayComponentType = ColumnMetaData.scalar(Types.INTEGER, "integer", Rep.INTEGER);
        ColumnMetaData arrayColumnMetaData = getArrayColumnMetaData(arrayComponentType, 2, "counts");
        List<ColumnMetaData> columns =
                Arrays.asList(MetaImpl.columnMetaData("str", 0, String.class, true));
        List<AvaticaParameter> params =
                Arrays.asList(
                        new AvaticaParameter(false, 10, 0, Types.VARCHAR, "VARCHAR",
                                String.class.getName(), "str"));
        Meta.CursorFactory cursorFactory = Meta.CursorFactory.create(Style.LIST, String.class,
                Arrays.asList("str"));
        // The row values
        List<Object> rows = new ArrayList<>();
        rows.add(new String[] {"str_value1"});
        rows.add(new String[] {"str_value2"});

        // Create the signature and frame using the metadata and values
        Signature signature = Signature.create(columns, sql, params, cursorFactory,
                Meta.StatementType.SELECT);
        Frame frame = Frame.create(0, true, rows);

        // And then create a ResultSetResponse
        ResultSetResponse results1 = new ResultSetResponse(connectionID, statementID, false,
                signature, frame, -1, rpcMetadata);
        responses.add(results1);

        responses.add(new CloseStatementResponse(rpcMetadata));

        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl(false, true,
                Integer.MAX_VALUE, "catalog", "schema");
        responses.add(new ConnectionSyncResponse(connProps, rpcMetadata));

        responses.add(new OpenConnectionResponse(rpcMetadata));
        responses.add(new CloseConnectionResponse(rpcMetadata));

        responses.add(new CreateStatementResponse(connectionID, Integer.MAX_VALUE, rpcMetadata));

        Map<Meta.DatabaseProperty, Object> propertyMap = new HashMap<>();
        for (Meta.DatabaseProperty prop : Meta.DatabaseProperty.values()) {
            propertyMap.put(prop, prop.defaultValue);
        }
        responses.add(new DatabasePropertyResponse(propertyMap, rpcMetadata));

        responses.add(
                new ExecuteResponse(Arrays.asList(results1, results1, results1), false, rpcMetadata));
        responses.add(new FetchResponse(frame, false, false, rpcMetadata));
        responses.add(new FetchResponse(frame, true, true, rpcMetadata));
        responses.add(new FetchResponse(frame, false, true, rpcMetadata));
        responses.add(
                new PrepareResponse(
                        new Meta.StatementHandle("connectionId", Integer.MAX_VALUE, signature),
                        rpcMetadata));

        StringWriter sw = new StringWriter();
        new Exception().printStackTrace(new PrintWriter(sw));
        responses.add(
                new ErrorResponse(Collections.singletonList(sw.toString()), "Test Error Message",
                        ErrorResponse.UNKNOWN_ERROR_CODE, ErrorResponse.UNKNOWN_SQL_STATE,
                        AvaticaSeverity.WARNING, rpcMetadata));

        // No more results, statement not missing
        responses.add(new SyncResultsResponse(false, false, rpcMetadata));
        // Missing statement, no results
        responses.add(new SyncResultsResponse(false, true, rpcMetadata));
        // More results, no missing statement
        responses.add(new SyncResultsResponse(true, false, rpcMetadata));

        // Some tests to make sure ErrorResponse doesn't fail.
        responses.add(new ErrorResponse((List<String>) null, null, 0, null, null, null));
        responses.add(
                new ErrorResponse(Arrays.asList("stacktrace1", "stacktrace2"), null, 0, null, null, null));

        long[] updateCounts = new long[]{1, 0, 1, 1};
        responses.add(
                new ExecuteBatchResponse("connectionId", 12345, updateCounts, false, rpcMetadata));

        return responses;
    }
*//*


}
*/
