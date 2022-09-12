import json
import logging
import asyncio
import aiohttp

logging.basicConfig(level=logging.DEBUG)

async def get_tasks(session, api, endpoints):
    tasks = []
    headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'
    }
    for ep in endpoints:
        tasks.append(session.get("{}{}".format(api, ep), ssl=False, headers=headers))
    return tasks


async def get_data(api, endpoints, destination_dir):
    async with aiohttp.ClientSession() as session:
        tasks = await get_tasks(session, api, endpoints)

        responses = await asyncio.gather(*tasks)
        for res in responses:
            # print(await res.json())
            file_name = str(res.url).split('/api/')[1].replace('/', '_')[:-1]
            with open("{}/{}.json".format(destination_dir, file_name), "w") as f:
                json.dump(await res.json(), f)
                logging.info("Data loaded into: {}.json".format(file_name))

with open("config.json", "r") as f:
    config = json.load(f)
    api = config['api']
    destination_dir = config['destination_dir']
    endpoints = config['endpoints']

asyncio.run(get_data(api, endpoints, destination_dir))


# def extract(uri, data_dir):
#     # Loading configuration data
#     with open("config.json", "r") as f:
#         config = json.load(f)
#     api = config['api']
#     destination_dir = config['destination_dir']
#     endpoints = config['endpoints']

#     for i in endpoints:
#         try:
#             response = requests.get(uri)
#             res_text = json.loads(response.text)
#             with open(data_dir, 'w') as f:
#                 json.dump(res_text, f)
#         except Exception as e:
#             logging.error(e)

# if __name__ == "__main__":
#     extract()