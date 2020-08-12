import pandas as pd
import requests

def check_updated_date(table_id):
    url = "http://data.ssb.no/api/v0/no/table/?query=title:" + table_id
    ssb_table_query = requests.get(url).json()
    return ssb_table_query[0]["published"]

def published_to_dataframe(table_id):
    data = [[table_id, check_updated_date(table_id)]]
    print(data)
    dataframe = pd.DataFrame(data, columns = ["Tabell Nummer", "Oppdatert Dato"])
    return dataframe

r = published_to_dataframe(TabellNummer)
print(r)
