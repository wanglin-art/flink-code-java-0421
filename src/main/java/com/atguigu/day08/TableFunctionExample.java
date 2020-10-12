package com.atguigu.day08;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * 王林
 * 2020/10/12 17点38分
 **/
public class TableFunctionExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<String> stream = env.fromElements("hello#world", "bigdata#atguigu");

        tEnv.createTemporaryView("t",stream,$("s"));


        //TABLE API
        Table firstTableResult = tEnv
                .from("t")
                .joinLateral(call(SplitFunction.class, $("s")))
                .select($("s"), $("word"), $("length"));

        Table secondTableResult = tEnv
                .from("t")
                .leftOuterJoinLateral(call(SplitFunction.class, $("s")))
                .select($("s"), $("word"), $("length"));

        Table thirdTableResult = tEnv
                .from("t")
                .leftOuterJoinLateral(call(SplitFunction.class, $("s")).as("newWord", "newLength"))
                .select($("s"), $("newWord"), $("newLength"));

        //SQL写法
        //注册UDF函数
        tEnv.createTemporarySystemFunction("SplitFunction",SplitFunction.class);
        Table firstSqlResult = tEnv.sqlQuery("select s , word, length from t,lateral table (SplitFunction(s))");

        Table secondSqlResult = tEnv.sqlQuery("select s , word, length from t left join lateral table (SplitFunction(s)) on true");


        tEnv.toAppendStream(secondSqlResult,Row.class).print();

        env.execute();
    }

    @FunctionHint(output = @DataTypeHint("ROW<word STRING,length INT>"))
    public static class SplitFunction extends TableFunction<Row>{

        public void eval(String str){
            for (String s : str.split("#")){
                collect(Row.of(s,s.length()));
            }
        }
    }
}
