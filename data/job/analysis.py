

hint = "/*+ JOIN_PREFIX(kind_type, link_type, comp_cast_type, role_type, company_type, info_type, movie_link, complete_cast, keyword, company_name, aka_title, movie_info_idx, aka_name, movie_companies, movie_keyword, char_name, title, name, person_info, movie_info, cast_info) */ "
with open("origin.sql", 'r') as f:
    with open("origin-hint.sql", 'w') as wf:
        for sql in f.readlines():
            wf.write(hint + sql)