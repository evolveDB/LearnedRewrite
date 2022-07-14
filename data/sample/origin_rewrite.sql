SELECT MIN(`t103`.`production_note`) AS `production_note`, MIN(`t104`.`movie_title`) AS `movie_title`, MIN(`t104`.`movie_year`) AS `movie_year`  FROM (SELECT `t100`.`movie_id`, `t101`.`movie_id` AS `movie_id0`, MIN(`t100`.`production_note`) AS `production_note`  FROM (SELECT `t96`.`id` AS `id0`, `t98`.`movie_id`, MIN(`t98`.`production_note`) AS `production_note`  FROM (SELECT `id`  FROM `company_type`  WHERE `kind` = 'production companies'  GROUP BY `id`) AS `t94`,  (SELECT `id`  FROM `info_type`  WHERE `info` = 'top 250 rank'  GROUP BY `id`) AS `t96`,  (SELECT `movie_id`, `company_type_id`, MIN(`note`) AS `production_note`  FROM `movie_companies`  WHERE (`note` LIKE '%(co-production)%' OR `note` LIKE '%(presents)%') AND `note` NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'  GROUP BY `movie_id`, `company_type_id`) AS `t98`  WHERE `t94`.`id` = `t98`.`company_type_id`  GROUP BY `t96`.`id`, `t98`.`movie_id`) AS `t100`,  (SELECT `movie_id`, `info_type_id`  FROM `movie_info_idx`  GROUP BY `movie_id`, `info_type_id`) AS `t101`  WHERE `t100`.`movie_id` = `t101`.`movie_id` AND `t100`.`id0` = `t101`.`info_type_id`  GROUP BY `t100`.`movie_id`, `t101`.`movie_id`) AS `t103`,  (SELECT `id`, MIN(`title`) AS `movie_title`, MIN(`production_year`) AS `movie_year`  FROM `title`  GROUP BY `id`) AS `t104`  WHERE `t104`.`id` = `t103`.`movie_id` AND `t104`.`id` = `t103`.`movie_id0`;
SELECT MIN(`t609`.`production_note`) AS `production_note`, MIN(`t611`.`movie_title`) AS `movie_title`, MIN(`t611`.`movie_year`) AS `movie_year`  FROM (SELECT `t607`.`movie_id`, `t608`.`movie_id` AS `movie_id0`, MIN(`t607`.`production_note`) AS `production_note`  FROM (SELECT `t604`.`id` AS `id0`, `t606`.`movie_id`, MIN(`t606`.`production_note`) AS `production_note`  FROM (SELECT `id`  FROM `company_type`  WHERE `kind` = 'production companies'  GROUP BY `id`) AS `t602`  CROSS JOIN (SELECT `id`  FROM `info_type`  WHERE `info` = 'bottom 10 rank'  GROUP BY `id`) AS `t604`  INNER JOIN (SELECT `movie_id`, `company_type_id`, MIN(`note`) AS `production_note`  FROM `movie_companies`  WHERE `note` NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'  GROUP BY `movie_id`, `company_type_id`) AS `t606` ON `t602`.`id` = `t606`.`company_type_id`  GROUP BY `t604`.`id`, `t606`.`movie_id`) AS `t607`  INNER JOIN (SELECT `movie_id`, `info_type_id`  FROM `movie_info_idx`  GROUP BY `movie_id`, `info_type_id`) AS `t608` ON `t607`.`movie_id` = `t608`.`movie_id` AND `t607`.`id0` = `t608`.`info_type_id`  GROUP BY `t607`.`movie_id`, `t608`.`movie_id`) AS `t609`  INNER JOIN (SELECT `id`, MIN(`title`) AS `movie_title`, MIN(`production_year`) AS `movie_year`  FROM `title`  WHERE `production_year` >= 2005 AND `production_year` <= 2010  GROUP BY `id`) AS `t611` ON `t609`.`movie_id` = `t611`.`id` AND `t609`.`movie_id0` = `t611`.`id`;
