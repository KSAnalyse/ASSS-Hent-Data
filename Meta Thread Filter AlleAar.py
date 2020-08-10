import pandas as pd
from pyjstat import pyjstat
import requests
from collections import OrderedDict
import time
import copy
import re
import json
from datetime import datetime
import multiprocessing
import signal


class SSBTable:
    """
    En klasse som brukes til å hente metadata fra ssb.no, behandle dem og holde på de variablene.
    
    ...

    Attributes:
    -----------
    table_id : str
        Tabellnummeret som brukes for å koble opp mot riktig tabell.
    metadata_filter : list
        En liste som er satt til None som standard, med mindre et filter har blitt sendt med.
        Dette filteret definerer om vi skal hente spesefikk data fra SSB.

    Methods:
    --------
    metadata_url(self)
        Lager en string basert på table_id og returnerer den.
        URLen den returnerer er addressen til tabellen vi gjør en spørring mot.
    metadata_variables(self, metadata_filter)
        Gjør en get request for metadataen i json og sjekker om vi har fått et filter
        Gjør et kall på filter_json_metadata() hvis det er filter med
        Returnerer metadataen.
    filter_json_metadata(self, json_metadata, filter_dict)
        ...
    filter_as_dict
        ...
    find_table_dimensions
        Finner og returner plasseringen til Region, Tid og kalkulerer 
        størrelsen på alle andre dimensjoner utenom Region og Tid.
        Den hopper over Omfang, da det den ikke brukes av KS.
    
    """

    def __init__(self, table_id, metadata_filter=None):
        """
        Parameters:
        -----------
        table_id : str
            Tabellnummeret som brukes for å koble opp mot riktig tabell.
        metadata_filter : list/None
            En liste som er satt til None som standard, med mindre et filter har blitt sendt med.
            Dette filteret definerer om vi skal hente spesefikk data fra SSB.
        
        Attributes:
        -----------
        variables : list
            Liste med alle metadataene fra tabell_id, har løst det sånn for at uthenting av
            data skjer bare engang.
        table_region : int
            Posisjonen til Region dimensjonen i variables listen
        table_tid : int
            Posisjonen til Tid dimensjonen i variables listen
        table_size : int
            Størrelsen til dimensjonene, utenom table_region, table_tid og omfang hvis 
            tabellen har en omfang dimensjon.
        ssb_max_row_query : int
            Største antall rader vi kan hente ut per spørring mot SSB

        """
        self.table_id = table_id
        self.metadata_filter = metadata_filter
        self.variables = self.metadata_variables(metadata_filter)
        self.table_region, self.table_tid, self.table_size = self.find_table_dimensions
        self.ssb_max_row_query = 800000

    @property
    def metadata_url(self):
        """
        Lager en string basert på table_id og returnerer den.
        URLen den returnerer er addressen til tabellen vi gjør en spørring mot.
        Returns:
        --------
        url : string
            Returnerer URLen til tabellen vi skal spørre mot
        """
        url = "http://data.ssb.no/api/v0/no/table/" + self.table_id
        return url

    def metadata_variables(self, metadata_filter):
        """
        Gjør en get request for metadataen i json og sjekker om vi har fått et filter
        Gjør et kall på filter_json_metadata() hvis det er filter med
        Returnerer metadataen.

        Parameters:
        -----------
        metadata_filter : None/list
            Tom ved default, liste med filter tags 

        Returns:
        --------
        filtered_variables : list
            Returnerer enten rå metadata eller filtrerte metadataen.
        """
        filtered_variables = []
        ssb_table_metadata = requests.get(self.metadata_url).json()
        if (metadata_filter != None):
            filtered_variables = self.filter_json_metadata(
                ssb_table_metadata, self.filters_as_dict(self.metadata_filter))
        else:
            filtered_variables = ssb_table_metadata
        return filtered_variables

    def filter_json_metadata(self, json_metadata, filter_dict):
        for idx, var in enumerate(json_metadata["variables"]):
            if var["code"] in filter_dict:
                value_texts = []
                for value in filter_dict[var["code"]]:
                    index_of_value = json_metadata["variables"][idx]["values"].index(
                        value)
                    value_texts.append(
                        json_metadata["variables"][idx]["valueTexts"][index_of_value])

                json_metadata["variables"][idx]["values"] = filter_dict[var["code"]]
                json_metadata["variables"][idx]["valueTexts"] = value_texts
        return json_metadata

    def filters_as_dict(self, filter_string):
        filters = {}
        filter_args = re.split("[=&]", filter_string)
        for idx, meta_filter in enumerate(filter_args):
            if ((idx % 2) == 0):
                filters[meta_filter] = filter_args[filter_args.index(
                    meta_filter) + 1].split(",")
        return filters

    @property
    def find_table_dimensions(self):
        """
        Finner og returner plasseringen til Region, Tid og kalkulerer 
        størrelsen på alle andre dimensjoner utenom Region og Tid.
        Den hopper over Omfang, da det den ikke brukes av KS.

        Returns:
        --------
        table_region : int
            Posisjonen til Region dimensjonen i variables listen
        table_tid : int
            Posisjonen til Tid dimensjonen i variables listen
        table_size : int
            Størrelsen til dimensjonene, utenom table_region, table_tid og omfang hvis 
            tabellen har en omfang dimensjon.
        """
        table_region = 0
        table_tid = 0
        table_size = 1
        for v_idx, var in enumerate(self.variables["variables"]):
            if var["text"] == "region":
                table_region = v_idx
            elif var["code"] == "Tid":
                table_tid = v_idx
            elif "omfang" in var["text"]:
                print("yes")
            else:
                table_size *= len(var["values"])
        return table_region, table_tid, table_size

class RegionKLASS:
    def __init__(self, klass_id):
        self.klass_id = klass_id
        self.to_date = time.localtime(time.time()).tm_year
        self.from_date = self.to_date - 5
        self.klass_variables = self.get_klass_variables()
        self.filtered_klass_variables = self.filter_klass_variables()
        self.filtered_regions = self.filter_regions()

    def region_klass_url(self, i):
        url = "http://data.ssb.no/api/klass/v1/classifications/" + i + "/codes?from=" + \
            str(self.from_date) + "-01-01&to=2059-01-01&includeFuture=true"
        return url

    def get_klass_variables(self):
        all_klass_data = []
        for i in self.klass_id:
            headers = {"Accept": "application/json", "charset": "UTF-8"}
            response = requests.get(self.region_klass_url(i), headers=headers)
            data = response.text

            if ("?" in response.text):
                data = response.text.replace("?", "š")
            all_klass_data.append(data)
        return all_klass_data

    def filter_klass_variables(self):
        regioner = []
        for klass_id in self.klass_variables:
            json_array = json.loads(klass_id)["codes"]

            for item in json_array:
                regioner_details = {"code": None,
                                    "validFrom": None, "validTo": None}
                regioner_details["code"] = item["code"]
                datetime_object = datetime.strptime(
                    item["validFromInRequestedRange"], "%Y-%m-%d")
                regioner_details["validFrom"] = str(datetime_object.year)
                datetime_object = datetime.strptime(
                    item["validToInRequestedRange"], "%Y-%m-%d")
                regioner_details["validTo"] = str(datetime_object.year)
                regioner.append(regioner_details)
        return regioner

    def filter_regions(self):
        filtered_regions_klass = {}
        for regions in self.filtered_klass_variables:
            try:
                a = filtered_regions_klass[regions["code"]]
                if regions["validFrom"] < filtered_regions_klass[regions["code"]]["validFrom"]:
                    filtered_regions_klass[regions["code"]
                                           ]["validFrom"] = regions["validFrom"]
                else:
                    filtered_regions_klass[regions["code"]
                                           ]["validTo"] = regions["validTo"]
            except KeyError:
                filtered_regions_klass[regions["code"]] = regions
        return filtered_regions_klass



def build_query(variables, _filter="item"):
    query = {
        "query": [],
        "response": {
            "format": "json-stat2"
        }
    }

    for var in variables:
        query_details = {
            "code": None,
            "selection": {
                "filter": _filter,
                "values": []
            }
        }

        query_details["code"] = var["code"]
        if (_filter != "item"):
            query_details["selection"]["filter"] = _filter
        if "omfang" in var["text"]:
            query_details["selection"]["values"] = ["A"]
        else:
            query_details["selection"]["values"].extend(var["values"])
        query["query"].append(query_details)
    return query

def meta_filter():
    metadata_filter = []
    
    for year in ssb_table.variables["variables"][ssb_table.table_tid]["values"]:
        new_meta_var = copy.deepcopy(ssb_table.variables["variables"])
        new_meta_regions = []
        for region in ssb_table.variables["variables"][ssb_table.table_region]["values"]:
            if region in {"0", "EAK", "EAKUO"}:
                new_meta_regions.append(region)
            elif region in klass.filtered_regions.keys():
                valid_from = int(klass.filtered_regions[region]["validFrom"])
                valid_to = int(klass.filtered_regions[region]["validTo"])
                if int(year) in range(valid_from, valid_to):
                    if (ssb_table.table_size * (len(new_meta_regions) + 1)) < ssb_table.ssb_max_row_query:
                        new_meta_regions.append(region)
                    else:
                        new_meta_var[ssb_table.table_region]["values"] = new_meta_regions
                        new_meta_var[ssb_table.table_tid]["values"] = [year]
                        metadata_filter.append(new_meta_var)
                        new_meta_regions = []
                        new_meta_regions.append(region)
                        new_meta_var = copy.deepcopy(
                            ssb_table.variables["variables"])
        new_meta_var[ssb_table.table_region]["values"] = new_meta_regions
        new_meta_var[ssb_table.table_tid]["values"] = [year]
        metadata_filter.append(new_meta_var)

    return metadata_filter

def run_pyjstat(result_list):
    return pyjstat.from_json_stat(result_list.json(object_pairs_hook=OrderedDict), naming="id")[0]
    

def post_query():    
    meta_data = meta_filter()
    result_list = []

    timer_for = time.time()
    for variables in meta_data:
        query = build_query(variables)
        data = requests.post(ssb_table.metadata_url, json=query)
        result_list.append(data)
        time.sleep(5.0)
        print(data)
    print("FOR LOOP: ", time.time() - timer_for)
    return result_list


def worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def master():
    dataframes = []
    timer2 = time.time()
    x = post_query()
    timer = time.time()
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
    print("1")
    dataframes = pool.map(run_pyjstat, x)
    print("2")
    print("3")
    pool.close()
    pool.join()
    print("THREADS: ", time.time() - timer)
    print("FULL QUERY: ", time.time() - timer2)
    big_df = pd.concat(dataframes, ignore_index=True)
    return big_df
        

if __name__ == "__main__":
    ssb_table = SSBTable("11820")
    klass = RegionKLASS(["131", "104", "214", "231"])
    r = master()
    print(r)