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
//    String path = System.getProperty("user.dir");
        JSONArray schemaJson = Utils.readJsonFile(args[0]);
        Rewriter rewriter = new Rewriter(schemaJson);

        BufferedReader reader = new BufferedReader(new FileReader(args[1]));
        BufferedWriter writer = new BufferedWriter(new FileWriter(args[2]));
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
            double originCost = rewriter.getCostRecordFromRelNode(originRelNode);
            Node rewrittenNode = new Node(originSql, originRelNode, (float) originCost, rewriter, (float) 0.1, null, "original_query");
            Node res = rewrittenNode.UTCSEARCH(20, rewrittenNode, 1);
            String rewrittenSql = res.state.replace("\n"," ").replace("\r"," ").strip();
            double rewrittenCost = rewriter.getCostRecordFromRelNode(res.state_rel);
            long end = System.currentTimeMillis();
            LOGGER.debug("time used(s):{}, index:{}, origin:{}, cost:{}, rewrite:{}, cost:{}",(end-start)/1000.0,idx, originSql, originCost, rewrittenSql,rewrittenCost);
            writer.write(rewrittenSql + ";\n");
            idx++;
        }
        reader.close();
        writer.close();
    }
}
