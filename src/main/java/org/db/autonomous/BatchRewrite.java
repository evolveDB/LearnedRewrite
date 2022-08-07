package org.db.autonomous;

import com.alibaba.fastjson.JSONArray;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;


public class BatchRewrite {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchRewrite.class);
    public static void main(String[] args) throws Exception {
        Utils.loadLogbackConfiguration(Utils.__CONF_DIR__);

        //Config
        // String path = System.getProperty("user.dir");
        JSONArray schemaJson = Utils.readJsonFile(args[0]);

        //DB Config
        String dbname = args[1];
        String host = args[2];
        String port = args[3];
        String user = args[4];
        String passwd= args[5];
        String dbDriver = "com.mysql.jdbc.Driver";

        /*
        String host = "166.111.121.62";
        String port = "3306";
        String user = "feng";
        String passwd= "db10204";
        String dbname = "imdbload";
        String dbDriver = "com.mysql.cj.jdbc.Driver";
        */

        DBConn db = new DBConn(host,port,user,passwd,dbname,dbDriver);

        // Rewriter rewriter = new Rewriter(schemaJson);
        Rewriter rewriter = new Rewriter(schemaJson,host,port,user,passwd,dbname,dbDriver);

        BufferedReader reader = new BufferedReader(new FileReader(args[6]));
        BufferedWriter writer = new BufferedWriter(new FileWriter(args[7]));
        String line = null;
        int idx = 0;
        while (null != (line = reader.readLine())) {
            line = line.strip();
            if (line.isEmpty()) {
                continue;
            }
            long start = System.currentTimeMillis();
            String originSql = line;
            RelNode originRelNode = rewriter.SQL2RA(originSql);

            // double originCost = rewriter.getCostRecordFromRelNode(originRelNode);
            double originCost = db.getCost(originSql);
            // guided by cost values

            Node rewrittenNode = new Node(originSql, originRelNode, (float) originCost, rewriter, (float) 0.1, null, "original_query", db);
            Node res = rewrittenNode.UTCSEARCH(50, rewrittenNode, 1);
            String rewrittenSql = res.state.replace("\n"," ").replace("\r"," ").strip();

            double rewrittenCost = db.getCost(res.state);
            long end = System.currentTimeMillis();
            LOGGER.debug("time used(s):{}, index:{}, origin:{}, cost:{}, rewrite:{}, cost:{}",(end-start)/1000.0,idx, originSql, originCost, rewrittenSql,rewrittenCost);
            writer.write(rewrittenSql + ";\n");
//            writer.write("time used(s):"+(end-start)/1000.0+", index: "+idx+", origin_cost: "+originCost+", rewrite_cost: "+rewrittenCost+", origin: "+originSql+", rewrite: "+rewrittenSql+"\n");
            idx++;
        }

        reader.close();
        writer.close();
    }
}
