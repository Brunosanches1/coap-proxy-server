import asyncio

import aiocoap.resource as resource
import aiocoap

import json
import sqlite3
import requests

con = sqlite3.connect("data/garden_data.sqlite")

try:
        c = con.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS garden_data
                 (id INTEGER PRIMARY KEY, garden_id INTEGER, dev_id INTEGER, date DATE CURRENT_DATE, temp INTEGER, humidity INTEGER,
                 light INTEGER, sent BOOLEAN)""")
        con.close()
except Exception as e:
    pass

con.close()

class Snapshots(resource.Resource):
    async def render_post(self, request):
        data = json.loads(request.payload.decode('utf8'))

        DAO.insert_data(data)

        return aiocoap.Message(content_format=0, payload="Ok".encode('utf8'))
        
class DAO():
    @staticmethod
    def insert_data(payload):
        try:
            con = sqlite3.connect("data/garden_data.sqlite")

            cur = con.cursor()
            query = 'INSERT INTO garden_data (garden_id, dev_id, temp, date, humidity, light, sent) VALUES (2, %d, %d, CURRENT_TIMESTAMP, %d, %d, FALSE)' \
                        % (payload['id'], payload['temperature'], payload['humidity'], payload['light'])

            cur.execute(query)
            con.commit()

            con.close()
        except:
            pass

    @staticmethod
    def get_data_not_sent():
        try:
            con = sqlite3.connect("data/garden_data.sqlite")

            cur = con.cursor()
            query = 'SELECT garden_id, dev_id as device,  temp AS temperature, humidity, light AS luminosity FROM garden_data WHERE sent is FALSE'

            cur.execute(query)

            r = [dict((cur.description[i][0], value) \
               for i, value in enumerate(row)) for row in cur.fetchall()]

            #query = 'UPDATE garden_data SET sent = TRUE WHERE sent=FALSE'

            r = (r[0] if r else None) if None else r

            garden_ids = set([x['garden_id'] for x in r])

            re_dict = []

            for gid in garden_ids:
                garden_data = {'garden_id': gid, 'data': []}

                garden_data['data'] = [dict([(key,value) for key, value in ri.items() if key != 'garden_id']) \
                                            for ri in r if ri['garden_id'] == gid]

                re_dict.append(garden_data)

            con.commit()

            con.close()

            return re_dict
        except:
            pass

@asyncio.coroutine
def periodic_send_to_cloud():
    while True:
        data_not_sent = DAO.get_data_not_sent()

        for data in data_not_sent:
            req = requests.post('http://0.0.0.0:80/api/v1/snapshots/%d' % data['garden_id'], json=data['data'])

        yield from asyncio.sleep(60)

def main():
    root = resource.Site()

    root.add_resource(['data'], Snapshots())

    asyncio.Task(aiocoap.Context.create_server_context(root))
    asyncio.Task(periodic_send_to_cloud())

    asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    main()
