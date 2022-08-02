
with open("origin_rewrite.sql", 'r') as f:

    content = f.read()
    content = content.split(',')

    cnt = 0
    for i,c in enumerate(content):
        if 'origin_cost' in content[i] and 'rewrite_cost' in content[i+1]:

            ocost = float(content[i].split(' ')[2])
            rcost = float(content[i+1].split(' ')[2])

            if ocost > rcost:
                cnt = cnt + 1
                print(ocost, rcost, (ocost-rcost)/ocost)

    print(str(cnt))
