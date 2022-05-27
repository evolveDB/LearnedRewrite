import com.alibaba.fastjson.JSONArray;
import main.DBConn;
import main.Node;
import main.Rewriter;
import main.Utils;
// import main.EquivCheck;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import com.google.gson.JsonObject;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.tuple.Pair;
import verify.verifyrelnode;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Vector;

//
// import verify.*;


public class test {
  public static void main(String[] args) throws Exception {

    //DB Config
    String path = System.getProperty("user.dir");
    JSONArray schemaJson = Utils.readJsonFile(path+"/src/main/schema.json");
    Rewriter rewriter = new Rewriter(schemaJson);


    //todo query formating
    String testSql = "";

//    testSql = "select * from ( select * from customer where c_custkey > 100) as c_all order by c_phone;";

//    testSql = "select distinct l_orderkey, sum(l_extendedprice + 3 + (1 - l_discount)) as revenue, o_orderkey, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' group by l_orderkey, o_orderkey, o_shippriority order by revenue desc, o_orderkey;";

//    testSql = "SELECT MAX(\"o_orderkey\" + 1 + 2) FROM \"orders\";";
//
//    testSql = "SELECT MAX(\"o_orderkey\" + (1 + 2)) FROM \"orders\";";
//
//    testSql = "select * from orders where o_orderpriority = 1 + 2";
//
//    testSql = "select int4ge(1,3);";
//
//    testSql = "select * from lineitem, (select * from orders where o_orderkey > 10) as v2 where v2.o_orderkey<100 and l_orderkey > 10";
//
//    testSql = "select * from orders where (o_orderpriority + o_orderkey > 10 and o_orderkey < 100+2) and (1999 + 1 < o_totalprice and o_orderpriority like 'abcd') ";
//
//    testSql = "select * from orders where 1 = 2;";


//    testSql = "select distinct l_orderkey, sum(l_extendedprice + 3 + (1 - l_discount)) as revenue, o_orderkey, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' group by l_orderkey, o_orderkey, o_shippriority order by revenue desc, o_orderkey;";
//     testSql = "select distinct c1.c_custkey as ck from customer c1, customer c2, orders o where c1.c_custkey = c2.c_custkey and c1.c_custkey = o.o_orderkey";

    testSql = "select max(distinct l_orderkey) from lineitem where exists(select max(c_custkey) from customer where c_custkey = l_orderkey group by c_custkey);";

    testSql = testSql.replace(";", "");
    RelNode testRelNode = rewriter.SQL2RA(testSql);
//    String output = rewriter.getRelNodeTreeJson(testRelNode).toJSONString();
//    String output = Utils.getConditionFromRelNode(testRelNode).toJSONString();
//    System.out.println("------:"+output);
    // String sql_re = testRelNode.explain;
    // JsonObject jsres = verify.verify(sql_re, testSql);
//    System.out.println(testSql);
    // System.out.println(sql_re);
    // System.out.println(jsres);

    double origin_cost = rewriter.getCostRecordFromRelNode(testRelNode);
//    System.out.println("origin_cost: " + origin_cost);

    Node resultNode = new Node(testSql,testRelNode, (float) origin_cost,rewriter, (float) 0.1,null,"original query");

    Node res = resultNode.UTCSEARCH(20, resultNode,1);
    System.out.println("ORI:"+testSql);
    System.out.println("REW:"+res.state);
    System.out.println("Original cost: "+origin_cost);
    System.out.println("Optimized cost: "+rewriter.getCostRecordFromRelNode(res.state_rel));
//    System.out.println("Tree Json: " + Utils.generate_json(resultNode));
//    Utils.dfs_mtcs_tree(res, 0);

//    System.out.println("--------Equality Check: Two SQL: -------------");
//    System.out.println(testSql);
//    System.out.println(res.state);
//    System.out.println("--------Equality Check: Two Relnodes: -------------");
//    JsonObject eqres = verifyrelnode.verifyrelnode(testRelNode, res.state_rel, testSql, res.state);
//    System.out.println("-------Equality Check Res: --------------");
//    System.out.println(eqres);
    // EquivCheck.checkeq(rewriter, testSql, res.state, testRelNode, res.state_rel);
  }
}
