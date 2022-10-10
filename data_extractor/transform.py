import json

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
            