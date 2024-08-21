import asyncio

import aiohttp
from more_itertools import chunked

from models import init_orm, Session, SwPeople


MAX_REQUESTS = 5

async def get_people(session, id):   
    response = await session.get(f"https://swapi.dev/api/people/{id}")
    json_data = await response.json()
    if response.status == 200:
        del json_data['created'], json_data['edited'], json_data['url']
        json_data['films'] = await get_films(session, json_data['films'])
        json_data['starships'] = await get_items(session, json_data['starships'])
        json_data['vehicles'] = await get_items(session, json_data['vehicles'])
        json_data['species'] = await get_items(session, json_data['species'])
        json_data['homeworld'] = await get_item_name(session, json_data['homeworld'])
        return json_data

async def get_film_title(session, url):
    response = await session.get(url)
    json_data = await response.json()
    film_name = json_data.get('title')
    return film_name

async def get_films(session, url_list):
    films = [get_film_title(session, url) for url in url_list]
    titles = await asyncio.gather(*films)
    if titles:
        return ', '.join(titles)
    return None

async def get_item_name(session, url):
    response = await session.get(url)
    json_data = await response.json()
    item_name = json_data.get('name')
    return item_name

async def get_items(session, url_list):
    items = [get_item_name(session, url) for url in url_list]
    names_list = await asyncio.gather(*items)
    if names_list:
        return ', '.join(names_list)
    return None

async def insert_json_list(json_list):
    
    async with Session() as db_session:
        orm_models = [SwPeople(**json) for json in json_list if json is not None]
        db_session.add_all(orm_models)
        await db_session.commit()

async def main():
    await init_orm()
    async with aiohttp.ClientSession() as session:
        for ids in chunked(range(1, 101), MAX_REQUESTS):
            coros = [get_people(session, id) for id in ids]
            json_list = await asyncio.gather(*coros)
            asyncio.create_task(insert_json_list(json_list))

    all_tasks = asyncio.all_tasks()
    main_task = asyncio.current_task()
    all_tasks.remove(main_task)
    await asyncio.gather(*all_tasks)

asyncio.run(main())
