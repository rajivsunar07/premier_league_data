import json
import logging

logging.basicConfig(level=logging.DEBUG)

destination_dir = ''
with open('config.json') as f:
    config = json.load(f)

    destination_dir = config['destination_dir']

data = []

with open(destination_dir + '/bootstrap-static.json') as f:
    data = json.load(f)

for i in data:
    with open("{}/{}.json".format(destination_dir, i), "w") as f:
        json.dump(data[i], f)
        logging.info("Data loaded into: {}.json".format(i))

with open(destination_dir + '/fixtures.json', 'r') as f:
    data = json.load(f)
    stats  = [{'code': x['code'], 'stats': x['stats']} for x in data]

with open(destination_dir + '/fixtures_stats.json', 'w') as f:
    json.dump(stats, f)
    logging.info("data loaded into: {}.json".format('fixtures_stats.json'))