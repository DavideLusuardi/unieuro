from algoliasearch.search_client import SearchClient, SearchIndex
import pprint
import pickle
import zlib
import datetime
import os
import sqlite3
import time


# TODO: use logging instead of print

REMOVED_AV, NON_AVAILABLE_AV, AVAILABLE_AV = -1, 0, 1

categories_lvl2 = {
    'inf:portatili': 'C12', # 'nbHits': 372,
    'inf:desktop': 'C13', # 'nbHits': 170,
    'inf:tablet': 'C14', # 'nbHits': 377,
    'inf:storage': 'C15', # 'nbHits': 157,
    'inf:stampanti': 'C16', # 'nbHits': 594,
    'inf:reti': 'C17', # 'nbHits': 339,
    'inf:accessori': 'C18', # 'nbHits': 944,
    'inf:gaming': 'C19', # 'nbHits': 503,
    'inf:componenti': 'C1C', # 'nbHits': 54,
    
    'tel:smartphone': 'C21', # 'nbHits': 628,
    'tel:accessori': 'C22', # 'nbHits': 1828,
    'tel:cordless': 'C23', # 'nbHits': 85,
    'tel:wearable': 'C24', # 'nbHits': 359,

    'tv:tv': 'C31', # 'nbHits': 427,
    'tv:decoder': 'C32', # 'nbHits': 45,
    'tv:dvd': 'C33', # 'nbHits': 17,
    'tv:home': 'C34', # 'nbHits': 92,
    'tv:accessori': 'C35', # 'nbHits': 416,

    'audio:portatile': 'C41', # 'nbHits': 123
    'audio:cuffie': 'C44', # 'nbHits': 189
    'audio:diffusori': 'C45', # 'nbHits': 201
    'audio:hifi': 'C42', # 'nbHits': 144
    'audio:dj': 'C43', # 'nbHits': 12
    'audio:accessori': 'C46', # 'nbHits': 109
    'audio:car': 'C52', # 'nbHits': 20
    'audio:navigatori': 'C51', # 'nbHits': 29

    'foto:fotocamere': 'C61', # 'nbHits': 93
    'foto:obiettivi': 'C62', # 'nbHits': 285
    'foto:videocamere': 'C65', # 'nbHits': 37
    'foto:ottica': 'C68', # 'nbHits': 3
    'foto:cornici': 'C66', # 'nbHits': 6
    'foto:accessori': 'C67', # 'nbHits': 457

    'elettrodomestici:grandi': 'C71', # 'nbHits': 1468
    'elettrodomestici:incasso': 'C72', # 'nbHits': 1146
    'elettrodomestici:aria': 'C73', # 'nbHits': 577
    'elettrodomestici:pulizia': 'C75', # 'nbHits': 848
    'elettrodomestici:cucina': 'C74', # 'nbHits': 927
    'elettrodomestici:illuminazione': 'C77', # 'nbHits': 646
    'elettrodomestici:casalinghi': 'C79', # 'nbHits': 1252
    'elettrodomestici:cancelleria': 'C78', # 'nbHits': 24
    'elettrodomestici:salute': 'C76', # 'nbHits': 840

    'console:ps5': 'C88', # 'nbHits': 214
    'console:ps4': 'C8P', # 'nbHits': 426
    'console:ps3': 'C81', # 'nbHits': 15
    'console:xboxseries': 'C8S', # 'nbHits': 106
    'console:xboxone': 'C8X', # 'nbHits': 120
    'console:xbox360': 'C85', # 'nbHits': 5
    'console:switch': 'C84', # 'nbHits': 293
    'console:wiiu': 'C8W', # 'nbHits': 5
    'console:3ds': 'C89', # 'nbHits': 29
    'console:wii': 'C82', # 'nbHits': 3
    'console:computer': 'C8C', # 'nbHits': 37

    'tlibero:droni': 'C83', # 'nbHits': 40
    'tlibero:mobilita': 'C8A', # 'nbHits': 303
    'tlibero:valigie': 'C8V', # 'nbHits': 69
    'tlibero:tlibero': 'C86', # 'nbHits': 512
    'tlibero:libri': 'C87', # 'nbHits': 33

}

category_groups = {
    'lvl2': [
        ['C12', 'C13', 'C14'],
        ['C16', 'C17'],
        ['C18'],
        ['C15', 'C19', 'C1C'],

        ['C21'],
        ['C23', 'C24'],

        ['C31', 'C32', 'C33', 'C34', 'C35'],

        ['C41', 'C44', 'C45', 'C42', 'C43', 'C46', 'C52', 'C51'],

        ['C61', 'C62', 'C65', 'C68', 'C66', 'C67'],

        ['C73', 'C78'],
        ['C74'],
        ['C75'],
        ['C76'],
        ['C77'],

        ['C88', 'C8P', 'C81', 'C8S'], 
        ['C8X', 'C85', 'C84', 'C8W', 'C89', 'C82', 'C8C'],

        ['C83', 'C8A', 'C8V', 'C86', 'C87'],
    ],

    'lvl3': [
        ['C2205', 'C2203', 'C2202'],
        ['C2207', 'C2208', 'C2206', 'C2204', 'C2209', 'C2210'],

        ['C7111', 'C7102', 'C7108', 'C7105', 'C7104'], 
        ['C7101', 'C7106', 'C7103', 'C7109', 'C7107'],

        ['C7201', 'C7205', 'C7208', 'C7203', 'C7207'], 
        ['C7209', 'C7202', 'C7204', 'C7206'],
        
        ['C7901', 'C7902', 'C7903', 'C7904', 'C7905', 'C7906'], 
        ['C7907', 'C7908', 'C7909', 'C7910', 'C7911', 'C7912'],
    ]
}


def init_db():
    connection = sqlite3.connect("hits.db")
    create_db_tables(connection)
    return connection

def create_db_tables(connection: sqlite3.Connection):
    cursor = connection.cursor()
    item_table = '''CREATE TABLE IF NOT EXISTS item (
        objectID TEXT NOT NULL PRIMARY KEY, 
        title TEXT, 
        originalPrice INTEGER,
        productUrl_it TEXT,
        imageUrl TEXT
        )'''
    cursor.execute(item_table)

    entry_table = '''CREATE TABLE IF NOT EXISTS entry (
        objectID TEXT NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,

        facetPrice INTEGER, 
        discountedPrice INTEGER, 
        discountPercentage INTEGER, 
        availability INTEGER NOT NULL,

        FOREIGN KEY (objectID) REFERENCES item (objectID),
        PRIMARY KEY (objectID, timestamp)
        )
        '''
    cursor.execute(entry_table)

def modify_db_tables(connection: sqlite3.Connection):
    ff = filter(lambda e: e.is_file(), os.scandir('hits/'))
    ff = sorted(list(map(lambda e: e.name, ff)))
    
    update_item_st = '''
        UPDATE item
        SET productUrl_it = ?, imageUrl = ?
        WHERE objectID = ?;
    '''

    for filename in ff:
        hits = parse_hits(os.path.join('hits/', filename))
        for hit in hits:
            connection.execute(update_item_st, (hit['productUrl_it'], hit['imageUrl'], hit['objectID']))

    connection.commit()
    print('item rows updated')

def update_db(connection: sqlite3.Connection, index: SearchIndex):
    hits = get_hits_categories(index, category_groups) # TODO
    # hits = parse_hits()

    entries = {}
    for hit in hits:
        # objectID = hit['objectID']
        title = hit['_highlightResult']['title_it']['value']
        # originalPrice = hit['originalPrice']
        # facetPrice = hit['facetPrice']
        # discountedPrice = hit['discountedPrice']
        # discountPercentage = hit['discountPercentage']
        if not hit['isAvailable']:
            availability = NON_AVAILABLE_AV
        else:
            # TODO: distinct between 'Disponibile' e previa disponibilità
            availability = AVAILABLE_AV
        
        entries[hit['objectID']] = (hit, title, availability)
    print(f"len entries: {len(entries)}")

    select_entries_query = '''
        SELECT e.*
        FROM entry e
        JOIN (SELECT objectID, MAX(timestamp) AS max_timestamp FROM entry GROUP BY objectID) em
        ON e.objectID=em.objectID
        AND e.timestamp=em.max_timestamp;
    '''
    res = connection.execute(select_entries_query)
    db_entries = res.fetchall()
    print(f"len db entries: {len(db_entries)}")
    db_objectIDs = set(map(lambda e: e[0], db_entries))
    # db_entries = dict([(e[0], e) for e in db_entries])

    insert_item_st = '''
        INSERT INTO item VALUES(?, ?, ?, ?, ?);
    '''

    insert_entry_st = '''
        INSERT INTO entry (objectID, facetPrice, discountedPrice, discountPercentage, availability)
        VALUES(?, ?, ?, ?, ?);
    '''

    removed_item_st = '''
        INSERT INTO entry (objectID, availability)
        VALUES(?, ?);
    '''

    for entry in db_entries:
        objectID_1, timestamp_1, facetPrice_1, discountedPrice_1, discountPercentage_1, availability_1 = entry
        if objectID_1 not in entries:
            # removed item
            print(f'removed item {objectID_1}')
            connection.execute(removed_item_st, (objectID_1, REMOVED_AV))
        else:
            hit, title_2, availability_2 = entries[objectID_1]
            if facetPrice_1 != hit['facetPrice'] or discountedPrice_1 != hit['discountedPrice'] or \
                discountPercentage_1 != hit['discountPercentage'] or availability_1 != availability_2:
                # update item
                print(f'update item {objectID_1} {title_2} avail={availability_2} {facetPrice_1}->{hit["facetPrice"]} {hit["discountPercentage"]}')
                connection.execute(insert_entry_st, (objectID_1, hit['facetPrice'], hit['discountedPrice'], hit['discountPercentage'], availability_2))

    for objectID in entries:
        if objectID not in db_objectIDs:
            # new item
            hit, title, availability = entries[objectID]
            print(f'new item {objectID} {title} avail={availability} {hit["facetPrice"]} {hit["discountPercentage"]}')
            connection.execute(insert_item_st, (objectID, title, hit['originalPrice'], hit['productUrl_it'], hit['imageUrl']))
            connection.execute(insert_entry_st, (objectID, hit['facetPrice'], hit['discountedPrice'], hit['discountPercentage'], availability))
    
    connection.commit()
    return hits


def print_nbHits_categories(index, categories):
    for c_name, c_id in categories.items():
        result = index.search('', {
            'hitsPerPage': 1000,
            'page': 0,
            'facetFilters': f'[["categories.lvl2:{c_id}"]]',
        })

        print(f'category: {c_name}')
        result['hits'] = None
        pprint.pprint(result)


def get_hits_categories(index, category_groups):
    hits = []
    for lvl, groups in category_groups.items():
        for group in groups:
            print(f'# {group}')
            facetFilters = ', '.join([f'"categories.{lvl}:{c_id}"' for c_id in group])
            result = index.search('', {
                'hitsPerPage': 1000,
                'page': 0,
                'facetFilters': f'[[{facetFilters}]]',
            })
            # pprint.pprint(result)
            # print(f"'nbHits': {result['nbHits']}")

            if result['nbHits'] > 1000:
                print(f"# ERROR: nbHits for group {group} is {result['nbHits']}")

            hits += result['hits']
    
    write_hits(hits) # TODO
    return hits

def write_hits(hits, filename=None):
    if filename is None:
        now = datetime.datetime.now()
        now = now.strftime("%Y-%m-%d_%H-%M-%S.%f")
        filename = f'hits/{now}.pkl.zip'

    with open(filename, 'wb') as f:
        f.write(zlib.compress(pickle.dumps(hits), zlib.Z_BEST_COMPRESSION))

def parse_hits(filename=None):
    if filename is None:
        ff = filter(lambda e: e.is_file(), os.scandir('hits/'))
        ff = sorted(list(map(lambda e: e.name, ff)))
        filename = os.path.join('hits/', ff[-1])

    with open(filename, 'rb') as f:
        hits = pickle.loads(zlib.decompress(f.read()))
    
    return hits

def get_discounted_items(hits, filename='discounted.txt'):
    discounted_items = []
    for hit in hits:
        title = hit['_highlightResult']['title_it']['value']
        objectID = hit['objectID']
        if hit['discountPercentage'] is not None:
            price = hit['discountedPrice']
            if not price:
                price = hit['facetPrice']
            discounted_items.append((hit['discountPercentage'], price, hit['isAvailable'], title, objectID, hit))

    discounted_items.sort(key=lambda i: i[0], reverse=True)
    # for item in discounted_items:
    #     print(item)

    return discounted_items

def get_low_price_items(hits, filename='low_price.txt'):
    low_price_items = []
    for hit in hits:
        title = hit['_highlightResult']['title_it']['value']
        objectID = hit['objectID']
        price = hit['discountedPrice']
        if not price:
            price = hit['facetPrice']
        
        if price:
            low_price_items.append((hit['discountPercentage'], price, hit['isAvailable'], title, objectID, hit))

    low_price_items.sort(key=lambda i: i[1], reverse=False)
    # for item in low_price_items:
    #     print(item)

    return low_price_items

def get_updated_items():
    select_query = '''
        SELECT *
        FROM entry
        WHERE timestamp BETWEEN '2023-08-11 00:00:01' AND '2023-08-11 23:59:59';
    '''
    res = connection.execute(select_query)
    entries = res.fetchall()
    for e in entries:
        objectID, timestamp, facetPrice, discountedPrice, discountPerccentage, availability = e


def get_by_id(index, objectID):
    result = index.search('', {
            'hitsPerPage': 1000,
            'page': 0,
            'filters': f'objectID:"{objectID}"',
            # 'facetFilters': '[["categories.lvl2:C15", "categories.lvl2:C16", "categories.lvl2:C17"]]',
        })
    if result['nbHits'] > 1:
        print(f"# ERROR::get_by_id::nbHits is {result['nbHits']}")

    return result['hits'][0]


def price_tracking(connection: sqlite3.Connection):
    client = SearchClient.create('MNBCENYFII', 'a79ae24a13bb82f4094119e7338fb70d')
    index = client.init_index('sgmproducts_prod')

    try:
        while True:
            hits = update_db(connection, index)
            print('db updated')
            # TODO
            items = get_discounted_items(hits)
            generate_web_page(items, filename='discounted.html')
            items = get_low_price_items(hits)
            generate_web_page(items, filename='low_price.html')
            
            print('sleep...')
            time.sleep(30*60)

    except KeyboardInterrupt:
        print('Program interrupted')
    finally:
        connection.close()
    
def generate_web_page(items, filename):
    table_data = []
    for item in items[:1000]:
        discountPercentage, price, isAvailable, title, objectID, hit = item
        if not isAvailable:
            continue

        table_data.append('<tr>')
        table_data.append(f'<td><a href="https://www.unieuro.it/online{hit["productUrl_it"]}"><img src="https://static1.unieuro.it{hit["imageUrl"]}"></a></td>')
        table_data.append(f'<td>{title[:50]}</td>')
        table_data.append(f'<td><b>{price}€</b></td>')
        table_data.append(f'<td>{discountPercentage}%</td>')
        # table_data.append(f'<td>{isAvailable}</td>')
        table_data.append(f'<td>{objectID}</td>')
        # table_data.append(f'<td><a href="https://www.unieuro.it/online{hit["productUrl_it"]}">link</a></td>')
        table_data.append('</tr>')
    
    page = '<html><body><table>'+'\n'.join(table_data)+'</table></body></html>'
    with open(filename, 'w') as f:
        f.write(page)


def main():
    secured_api_key = 'NDJkZDY5NjQxYWE0MmMzMmY0YWNmZTIxNDI2ODdkODRjYWI4OTEzYjE5Y2JkYmFhYzE3ZjkyNTIyMjM3YTBkMnVzZXJUb2tlbj0wJnZhbGlkVW50aWw9MTY5MTY1OTU1Mg=='
    client = SearchClient.create('MNBCENYFII', 'a79ae24a13bb82f4094119e7338fb70d')
    index = client.init_index('sgmproducts_prod')

    hit = get_by_id(index, 'TOGWR7915UPROQ')
    pprint.pprint(hit)
    return

    # for k,c in categories_lvl2.items():
    #     print(k)
    #     hits = get_hits_categories(index, [[c]])

    test_c = {
        'lvl3': [
            ['C2205', 'C2203', 'C2202'],
            ['C2207', 'C2208', 'C2206', 'C2204', 'C2209', 'C2210'],

            # ['C7111', 'C7102', 'C7108', 'C7105', 'C7104'], 
            # ['C7101', 'C7106', 'C7103', 'C7109', 'C7107'],

            # ['C7201', 'C7205', 'C7208', 'C7203', 'C7207'], 
            # ['C7209', 'C7202', 'C7204', 'C7206'],
            
            # ['C7901', 'C7902', 'C7903', 'C7904', 'C7905', 'C7906'], 
            # ['C7907', 'C7908', 'C7909', 'C7910', 'C7911', 'C7912'],
        ]
    }
    hits = get_hits_categories(index, test_c)
    print(len(hits))

    # hits = get_hits_categories(index, category_groups)
    # write_hits(hits)
    # print(len(hits))


if __name__ == '__main__':
    connection = init_db()
    price_tracking(connection)

    # main()

    # modify_db_tables(connection)

    # hits = parse_hits()
    # print(len(hits))
    # items = get_discounted_items(hits)
    # items = get_low_price_items(hits)
    # generate_web_page(items, filename='index.html')

    # get_discounted_items(hits)
    # get_low_price_items(hits)

    # items = parse_items('log_group_complete.txt')
    # print(len(items))


