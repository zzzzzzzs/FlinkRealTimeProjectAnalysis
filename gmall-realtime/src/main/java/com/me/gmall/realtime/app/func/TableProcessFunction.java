package com.me.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.me.gmall.realtime.bean.TableProcess;
import com.me.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Author: Felix
 * Date: 2021/5/13
 * Desc:  用于业务数据动态分流的函数类
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private OutputTag<JSONObject> dimTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //建立连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    public TableProcessFunction(OutputTag<JSONObject> dimTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.dimTag = dimTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }


    /**
     * @Description: 处理主流数据
     * @Param:
     * jsonObj: {"database":"gmallFlinkRealTime","xid":20051,"data":{"birthday":"1972-05-11","gender":"F","create_time":"2021-05-11 22:58:38","login_name":"q5zn6e9tiqj","nick_name":"茜茜","name":"薛茜","user_level":"2","phone_num":"13472383739","id":1,"email":"q5zn6e9tiqj@0355.net"},"xoffset":0,"type":"insert","table":"user_info","ts":1620917919}
     * {
     * 	"database": "gmallFlinkRealTime",
     * 	"xid": 20051,
     * 	"data": {
     * 		"birthday": "1972-05-11",
     * 		"gender": "F",
     * 		"create_time": "2021-05-11 22:58:38",
     * 		"login_name": "q5zn6e9tiqj",
     * 		"nick_name": "茜茜",
     * 		"name": "薛茜",
     * 		"user_level": "2",
     * 		"phone_num": "13472383739",
     * 		"id": 1,
     * 		"email": "q5zn6e9tiqj@0355.net"
     *        },
     * 	"xoffset": 0,
     * 	"type": "insert",
     * 	"table": "user_info",
     * 	"ts": 1620917919
     * }
     *
     * @return:
     * @auther: zzzzzzzs
     * @Date: 2021/5/13
     */
    // 如果主流没有数据，也就是业务表没有数据变化，此方法不会执行
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> tableProcessState = ctx.getBroadcastState(mapStateDescriptor);
        System.out.println("主流" + jsonObj.toJSONString());
        //获取表名
        String tableName = jsonObj.getString("table");
        //获取操作类型
        String type = jsonObj.getString("type");

        //注意：如果使用maxwell采集历史数据，那么操作类型是bootstrap-insert，需要改为insert
        if ("bootstrap-insert".equals(type)) {
            type = "insert";
            jsonObj.put("type", type);
        }
        //拼接查询的key
        String key = tableName + ":" + type;
        //根据key去广播状态中查询对应的配置信息
        TableProcess tableProcess = tableProcessState.get(key);
        if (tableProcess != null) {
            String sinkTable = tableProcess.getSinkTable();
            jsonObj.put("sink_table", sinkTable);

            //根据配置表的配置   对字段进行过滤
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            String sinkColumns = tableProcess.getSinkColumns();
            if(sinkColumns!=null && sinkColumns.length() > 0){
                filterColnmu(dataJsonObj,sinkColumns);
            }

            //找到配置信息了  进行分流
            if (tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)) {
                //维度数据 --发送到维度侧输出流中
                ctx.output(dimTag, jsonObj);
            } else if (tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)) {
                //事实数据---发送到主流
                out.collect(jsonObj);
            }
        } else {
            //没有找到配置信息
            System.out.println("No this Key in Table Process: " + key);
        }
    }


    //对字段进行过滤
    private void filterColnmu(JSONObject dataJsonObj, String sinkColumns) {
        //对sinkColumns进行分割，得到是保留的字段的名称属性
        String[] fileds = sinkColumns.split(",");
        List<String> fieldList = Arrays.asList(fileds);
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        entrySet.removeIf(ele->!fieldList.contains(ele.getKey()));
    }

    /**
     * @Description: 处理广播流数据()
     * @Param:
     *  jsonStr {"database":"gmallFlinkRealTimeDIM","data":{"operate_type":"insert","sink_type":"hbase","sink_table":"base_trademark","source_table":"base_trademark","sink_columns":"id,tm_name,logo_url"},"type":"insert","table":"table_process"}
     *
     * @return:
     * @auther: zzzzzzzs
     * @Date: 2021/5/13
     */
    // 如果配置表没有变化，此方法不会执行
    // TODO 广播流中的数据不需要发送到主流中
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
        System.out.println("广播流：" + jsonStr);
        //将读取到FlinkCDC采集到的信息  由jsonStr->jsonObj
        // {"database":"gmallFlinkRealTimeDIM","data":{"operate_type":"insert","sink_type":"hbase","sink_table":"base_trademark","source_table":"base_trademark","sink_columns":"id,tm_name,logo_url"},"type":"insert","table":"table_process"}
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        //获取data数据  也就是每一条配置信息 {"operate_type":"insert","sink_type":"hbase","sink_table":"base_trademark","source_table":"base_trademark","sink_columns":"id,tm_name,logo_url"}
        String dataJsonStr = jsonObj.getString("data");
        //将读取到的配置信息转换为TableProcess对象
        TableProcess tableProcess = JSON.parseObject(dataJsonStr, TableProcess.class);

        //获取源表表名
        String sourceTable = tableProcess.getSourceTable();
        //获取操作类型
        String operateType = tableProcess.getOperateType();
        //输出类型      hbase|kafka
        String sinkType = tableProcess.getSinkType();
        //输出目的地表名或者主题名
        String sinkTable = tableProcess.getSinkTable();
        //输出字段
        String sinkColumns = tableProcess.getSinkColumns();
        //表的主键
        String sinkPk = tableProcess.getSinkPk();
        //建表扩展语句
        String sinkExtend = tableProcess.getSinkExtend();
        //拼接key 表名+操作类型
        String key = sourceTable + ":" + operateType;

        //如果是维度类型配置，且是插入数据需要创建表
        // 如果将删除配置表中的数据会报空指针异常
        if (sinkType.equals(TableProcess.SINK_TYPE_HBASE) && "insert".equals(operateType)) {
            //通过Phoenix创建表
            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
        }

        //获取广播状态
        BroadcastState<String, TableProcess> tableProcessState = ctx.getBroadcastState(mapStateDescriptor);
        // 把key和配置表中的对象放到广播状态中
        tableProcessState.put(key, tableProcess);
    }

    //创建维度表
    private void checkTable(String tableName, String fieldStr, String pk, String ext) {
        if (ext == null) {
            ext = "";
        }
        // 没有主键就创建主键
        if (pk == null) {
            pk = "id";
        }
        // 拼接sql字符串
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        String[] fieldArr = fieldStr.split(",");
        for (int i = 0; i < fieldArr.length; i++) {
            String field = fieldArr[i];
            if (pk.equals(field)) {
                createSql.append(field + " varchar primary key ");
            } else {
                createSql.append("info." + field + " varchar ");
            }
            if (i < fieldArr.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(")" + ext);

        System.out.println("Phoenix建表语句:" + createSql.toString());
        //创建表
        PreparedStatement ps = null;
        try {
            //创建数据库操作对象
            ps = conn.prepareStatement(createSql.toString());
            //执行SQL语句
            ps.execute();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //释放资源
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

