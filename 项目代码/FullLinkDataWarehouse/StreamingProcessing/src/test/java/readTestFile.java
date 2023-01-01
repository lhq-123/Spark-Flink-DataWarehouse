import com.alex.project.utils.ConfigLoader;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * @author Alex_liu
 * @create 2023-01-01 0:44
 * @Description
 */
public class readTestFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env= StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.readTextFile(
//                "C:\\Users\\admin\\Desktop" + "\\Flink+Spark大型数仓\\Spark-Flink-data-warehouse\\项目代码\\FullLinkDataWarehouse\\StreamingProcessing\\src\\main\\java\\com\\alex\\project\\app\\ods\\table_process.txt")
//                .print("TextData>>>>>>");
        // 每隔10s中读取 hdfs上新增文件内容
        env.readFile(new TextInputFormat(new Path()),"C:\\Users\\admin\\Desktop\\Flink+Spark大型数仓\\Spark-Flink-data-warehouse\\项目代码\\FullLinkDataWarehouse\\StreamingProcessing\\src\\main\\java\\com\\alex\\project\\app\\ods\\table_process.txt"
                , FileProcessingMode.PROCESS_CONTINUOUSLY,10).print("配置流数据>>>>>");
        env.execute("readTextFile");
    }
}
