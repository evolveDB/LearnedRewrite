package org.db.autonomous;

import com.alibaba.fastjson.JSONArray;
import org.apache.calcite.rel.RelNode;

import java.io.InputStream;

public class test {
    public static void main(String[] args) throws Exception {

        //Config
        //String path = System.getProperty("user.dir");
        //InputStream inputStream = Utils.class.getResourceAsStream("/schema.json");
        JSONArray schemaJson = Utils.readJsonFile("data/job/imdb_schema.json");

        //DB Config
        String host = "166.111.121.62";
        String port = "3306";
        String user = "feng";
        String passwd= "db10204";
        String dbname = "imdbload";
        String dbDriver = "com.mysql.cj.jdbc.Driver";
        // org.db.autonomous

        //Rewriter rewriter = new Rewriter(schemaJson);
        Rewriter rewriter = new Rewriter(schemaJson,host,port,user,passwd,dbname,dbDriver);

        DBConn db = new DBConn(host,port,user,passwd,dbname,dbDriver);

        //todo query formating
        // String testSql = "select * from orders where (o_orderpriority + o_orderkey > 10 and o_orderkey < 100+2) and (1999 + 1 < o_totalprice and o_orderpriority like 'abcd')";
        //String testSql = "SELECT MIN(t.title) AS complete_downey_ironman_movie FROM complete_cast AS cc, comp_cast_type AS cct1, comp_cast_type AS cct2, char_name AS chn, cast_info AS ci, keyword AS k, kind_type AS kt, movie_keyword AS mk, name AS n, title AS t WHERE cct1.kind = 'cast' AND cct2.kind LIKE '%complete%' AND chn.name NOT LIKE '%Sherlock%' AND (chn.name LIKE '%Tony%Stark%' OR chn.name LIKE '%Iron%Man%') AND k.keyword IN ('superhero', 'sequel', 'second-part', 'marvel-comics', 'based-on-comic', 'tv-special', 'fight', 'violence') AND kt.kind = 'movie' AND n.name LIKE '%Downey%Robert%' AND t.production_year > 2000 AND kt.id = t.kind_id AND t.id = mk.movie_id AND t.id = ci.movie_id AND t.id = cc.movie_id AND mk.movie_id = ci.movie_id AND mk.movie_id = cc.movie_id AND ci.movie_id = cc.movie_id AND chn.id = ci.person_role_id AND n.id = ci.person_id AND k.id = mk.keyword_id AND cct1.id = cc.subject_id AND cct2.id = cc.status_id;";
        String testSql = "select * from aka_name";

        RelNode testRelNode = rewriter.SQL2RA(testSql);
        //double origin_cost = rewriter.getCostRecordFromRelNode(testRelNode);
        double origin_cost = db.getCost(testSql);

        Node resultNode = new Node(testSql, testRelNode, (float) origin_cost, rewriter, (float) 0.1, null, "original query",db);

        Node res = resultNode.UTCSEARCH(30, resultNode, 1);

        double new_cost = db.getCost(res.state);

        System.out.println("origin_sql:" + testSql);
        System.out.println("rewrite_sql:" + res.state);
        System.out.println("Original cost: " + origin_cost);
        System.out.println("Optimized cost: " + new_cost);
        System.out.println("Optimized rate: " + (origin_cost-new_cost)/origin_cost);
//      System.out.println(Utils.generate_json(resultNode));
    }
}