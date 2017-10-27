package distribute_transaction.scheduler;

/**
 * 目前封锁资源类型只支持String、Integer、Double、Float四种
 * Created by swqsh on 2017/9/28.
 */
class TableResourceFactory {

    static TableResource newTableResource(String tableName, String typeName){
        if(typeName.equalsIgnoreCase("String")){
            return newStringTableResource(tableName);
        }

        if(typeName.equalsIgnoreCase("Integer")||typeName.equalsIgnoreCase("int")){
            return newIntTableResource(tableName);
        }

        if(typeName.equalsIgnoreCase("Double")){
            return newDoubleTableReosurce(tableName);
        }

        if(typeName.equalsIgnoreCase("Float")){
            return newFloatTableResource(tableName);
        }

        if(typeName.equalsIgnoreCase("Long")){
            return newLongTableResource(tableName);
        }
        return null;
    }

    private static TableResource<Long> newLongTableResource(String tableName) {
        return new TableResource<Long>(tableName);
    }

    private static TableResource<Float> newFloatTableResource(String tableName) {
        return new TableResource<Float>(tableName);
    }

    private static TableResource<Double> newDoubleTableReosurce(String tableName) {
        return new TableResource<Double>(tableName);
    }

    private static TableResource<Integer> newIntTableResource(String tableName) {
        return new TableResource<Integer>(tableName);
    }

    private static TableResource<String> newStringTableResource(String tableName){
        return new TableResource<String>(tableName);
    }

}
