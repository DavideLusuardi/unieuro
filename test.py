from algoliasearch.search_client import SearchClient
import pprint


client = SearchClient.create('MNBCENYFII', 'a79ae24a13bb82f4094119e7338fb70d')
index = client.init_index('sgmproducts_prod')

# Initialise lists to store the exported data
records, settings, rules, synonyms = [], [], [], []

# Get the index records and any associated rules, settings, and synonyms.
try:
    # Get all the records from the index and store them in a list
    print('Retrieving records...')
    records = list(index.browse_objects())
    print(f'{len(records)} records retrieved.')

    # Get the settings for the index
    print('Retrieving settings...')
    settings = index.get_settings()
    print('Settings retrieved.')

    # Get the rules for the index
    print('Retrieving rules...')
    rules = list(index.browse_rules())
    print(f'{str(len(rules))} rules retrieved.')

    # Get the synonyms for the index
    print('Retrieving synonyms...')
    synonyms = list(index.browse_synonyms())
    print(f'{str(len(synonyms))} synonyms retrieved.')
except Exception as e:
    print(f'Error retrieving data: {e}')
    exit()