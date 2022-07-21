SELECT MIN("t"."name") AS "from_company", MIN("t"."link") AS "movie_link_type", MIN("t0"."title") AS "non_polish_sequel_movie"
FROM (SELECT *
      FROM
           (SELECT NULL AS "id", NULL AS "name", NULL AS "country_code", NULL AS "imdb_id", NULL AS "name_pcode_nf", NULL AS "name_pcode_sf", NULL AS "md5sum", NULL AS "id0", NULL AS "kind", NULL AS "id1", NULL AS "keyword", NULL AS "phonetic_code", NULL AS "id2", NULL AS "link", NULL AS "id3", NULL AS "movie_id", NULL AS "company_id", NULL AS "company_type_id", NULL AS "note", NULL AS "id4", NULL AS "movie_id0", NULL AS "keyword_id", NULL AS "id5", NULL AS "movie_id1", NULL AS "linked_movie_id", NULL AS "link_type_id") AS "t"
      WHERE 1 = 0) AS "t"
         INNER JOIN (SELECT *
                     FROM "title"
                     WHERE "production_year" >= 1950 AND "production_year" <= 2000) AS "t0"
             ON "t"."movie_id1" = "t0"."id" AND "t"."movie_id0" = "t0"."id" AND "t"."movie_id" = "t0"."id";