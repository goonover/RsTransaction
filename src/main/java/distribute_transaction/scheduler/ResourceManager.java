package distribute_transaction.scheduler;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 负责资源的分配
 * Created by swqsh on 2017/9/4.
 */
class ResourceManager {
    //配置文件路径
    private String configPath;

    /**
     * 参与分布式事务的每张表都应该拥有相应的表id以及逻辑资源表，
     * 通过逻辑资源表来分配事务获取表的资源
     */
    private Map<String,TableResource> tableResourceMap = new HashMap<String,TableResource>();

    ResourceManager(String configPath) {
        this.configPath = configPath;
        try {
            configParse();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private void configParse() throws IOException {
        JsonParser jsonParser = new JsonParser();
        String configStr = new String(Files.readAllBytes(Paths.get(configPath)));
        JsonArray jsonArray = jsonParser.parse(configStr).getAsJsonArray();
        for(int i=0;i<jsonArray.size();i++){
            JsonObject tableConfig = jsonArray.get(i).getAsJsonObject();
            String tableName = tableConfig.get("tableName").getAsString();
            String typeName = tableConfig.get("type").getAsString();
            TableResource tableResource = TableResourceFactory.newTableResource(tableName,typeName);
            if(tableResource != null){
                tableResourceMap.put(tableName,tableResource);
            }
        }
    }


    void schedule(TransactionImpl transaction){
        List<Range> keyRanges = transaction.getApplyRanges();
        while (keyRanges.size()>0){
            Range range = keyRanges.remove(0);
            TableResource resource = tableResourceMap.get(range.getTableName());
            if(resource!=null){
                resource.applyFor(range);
            }
        }
        transaction.firstAllocatedCompleted();
    }

    TableResource getTableResource(String resourceName){
        return tableResourceMap.get(resourceName);
    }

}
