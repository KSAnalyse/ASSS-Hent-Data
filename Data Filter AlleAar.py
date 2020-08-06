import pandas as pd
from pyjstat import pyjstat
import requests
from collections import OrderedDict
import time
import copy
import re
from io import StringIO
import json
from datetime import datetime
import numpy as np


class SSBTable:

    def __init__(self, table_id, meta_data_filter=None):
        """
        Initialiserer SSBTable, med en parameter.

        Variabler:
        table_id -- tabellnummer som blir brukt av SSB for å identifisere forskjellige tabellene.
        """
        self.table_id = table_id
        self.meta_data_filter = meta_data_filter
        self.variables = self.metadata_variables(meta_data_filter)
        self.total_rows, self.row_size = self.calculate_total_rows
        self.ssb_max_row_query = 800000
        self.dimension_iterate = self.dimension_to_iterate_on()

    """
    Tabellnummeret "table_id" blir lagt til url adressen til SSB API for tabeller.
    Variabler:
    url -- url er standard API addressen til tabeller.
    """
    @property
    def metadata_url(self):
        url = "http://data.ssb.no/api/v0/no/table/" + self.table_id
        return url

    """
    Henter ut metadata tabellen til table_id, som er i JSON-STAT2 format, den blir så splittet opp 
    fra tittel og variables. Full tittel kan i noen tilfeller bli for lang, så denne blir ikke tatt med.

    "metadata_variables" blir brukt av metadata_dimensions for å ha en egen liste over dimensjonene til tabellen
    og så av "calculate_total_rows" for å finne ut hvor mange rader tabellen vi skal bruke har.
    Les "calculate_total_rows" for mer informasjon om den funksjonen.

    Variabler:
    ssb_table_metadata -- JSON-Stat dokumentet blir lagret i en lokal variabel.
    ssb_variables_metadata_df -- Splitter så opp JSON dokumentet sånn at tittel ikke blir tatt med.
    """
    def metadata_variables(self, meta_data_filter):
        filtered_variables = []
        ssb_table_metadata = requests.get(self.metadata_url).json()
        if (meta_data_filter != None):
            filtered_variables = self.filter_json_metadata(
                ssb_table_metadata, self.filters_as_dict(self.meta_data_filter))
        else:
            filtered_variables = ssb_table_metadata
        """if (meta_data_filter != None):
            filters = {}
            filter_args = re.split("[=&]", meta_data_filter)
            for idx, meta in enumerate(filter_args):
                if ((idx % 2) == 0):
                    filters[meta] = filter_args[filter_args.index(meta) + 1].split(",")

            counter = 0
            for f in filters:
                var = {
                    "code": ssb_variables_metadata_df[counter]["code"],
                    "text": ssb_variables_metadata_df[counter]["text"],
                    "values": [],
                    "valueTexts": [],
                    "time": "true"
                }
                for l in filters[f]:
                    i = ssb_variables_metadata_df[counter]["values"].index(l)
                    var["values"].append(ssb_variables_metadata_df[counter]["values"][i])
                    var["valueTexts"].append(ssb_variables_metadata_df[counter]["valueTexts"][i])
                counter += 1
                filtered_variables.append(var)
        else:
            filtered_variables = ssb_variables_metadata_df"""
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

    """
    Kalkurerer total antall rader tabellen vi gjør spørring mot har. 
    Dette er på grunn av begrensning fra SSB API"et sin side 
    som ikke lar oss hente ut mer enn 800.000 rader per spørring.
    """
    @property
    def calculate_total_rows(self):

        total_rows = 1
        row_size = []
        for i in self.variables["variables"]:
            total_rows *= len(i["values"])
            row_size.append(len(i["values"]))
        return total_rows, row_size

    def dimension_to_iterate_on(self):
        total_div_by_row = []
        dimension_closest_to_max = 0
        for row_idx, row in enumerate(self.row_size):
            total_div_by_row.append(self.total_rows/row)
            if total_div_by_row[row_idx] < self.ssb_max_row_query and total_div_by_row[row_idx] > total_div_by_row[dimension_closest_to_max]:
                dimension_closest_to_max = row_idx
        return self.variables["variables"][dimension_closest_to_max]


class RegionKLASS:
    def __init__(self, klass_id):
        self.klass_id = klass_id
        self.to_date = time.localtime(time.time()).tm_year
        self.from_date = self.to_date - 5
        self.klass_variables = self.get_klass_variables()
        self.filtered_klass_variables = self.filter_klass_variables()
        self.filtered_regions = self.filter_regions()

    
    def region_klass_url(self, i):
        url = "http://data.ssb.no/api/klass/v1/classifications/" + i + "/codes?from=" + str(self.from_date) + "-01-01&to=2059-01-01&includeFuture=true"
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
                    filtered_regions_klass[regions["code"]]["validFrom"] = regions["validFrom"]
                else:
                    filtered_regions_klass[regions["code"]]["validTo"] = regions["validTo"]
            except KeyError:
                filtered_regions_klass[regions["code"]] = regions
        return filtered_regions_klass
        

ssb_table = SSBTable("07459", "Tid=2015,2016,2017,2018,2019")
klass = RegionKLASS(["131", "104", "214"])


def build_query(iterator=0, _filter="item", ):
    query = {
        "query": [],
        "response": {
            "format": "json-stat2"
        }
    }

    for i in ssb_table.variables["variables"]:
        period = []
        
        if (i["code"] == "Tid"):
            period = list(reversed(i["values"].copy()))
        query_details = {
            "code": None,
            "selection": {
                "filter": _filter,
                "values": []
            }
        }
        if ssb_table.total_rows < ssb_table.ssb_max_row_query:
            query_details["code"] = i["code"]
            if(_filter != "item"):
                query_details["selection"]["filter"] = _filter
            query_details["selection"]["values"].extend(i["values"])
            query["query"].append(query_details)
        elif (i["code"] != ssb_table.dimension_iterate["code"]):
            query_details["code"] = i["code"]
            if (_filter != "item"):
                query_details["selection"]["filter"] = _filter
            query_details["selection"]["values"].extend(i["values"])
            query["query"].append(query_details)
        elif i["code"] == ssb_table.dimension_iterate["code"]:
            query_details["code"] = i["code"]
            if (_filter != "item"):
                query_details["selection"]["filter"] = _filter
            if i["code"] == "Tid":
                query_details["selection"]["values"].append(period[iterator])
            else:
                query_details["selection"]["values"].append(i["values"][iterator])
                
            query["query"].append(query_details)

    return query


def data_filter(data):
    dataframes = []
    
    result = copy.deepcopy(data)
    
    size = data["size"]
    region_value_size = 1
    region_div = 1
    region_value_pos = 0
    table_region = 0
    table_tid = 0
    
    for r_idx, region in enumerate(ssb_table.variables["variables"]):
        if region["text"] == "region":
            table_region = r_idx
        elif region["code"] == "Tid":
            region_value_size *= size[r_idx]
            table_tid = r_idx
        else:
            region_value_size *= size[r_idx]
            region_div *= size[r_idx]
    
    data_region_index = data["dimension"][data["id"][table_region]]["category"]["index"]
    data_year_index = data["dimension"][data["id"][table_tid]]["category"]["index"]
    for y_idx, year in enumerate(data_year_index):
        counter = 0
        region_value_pos = y_idx
        region_value_range = region_value_size
        region_index = {}
        region_label = {}
        year_index = {}
        year_label = {}
        result["value"] = []
        for r_idx, region in enumerate(data_region_index):
            if region in {"0", "EAK", "EAKUO"}:
                region_index[region] = counter
                region_label[region] = data["dimension"][data["id"][table_region]]["category"]["label"][region]
                year_index[year] = 0
                year_label[year] = year
                for val in data["value"][region_value_pos:region_value_range:size[table_tid]]:
                    result["value"].append(val)
                counter += 1
            elif region in klass.filtered_regions.keys():
                valid_from = int(klass.filtered_regions[region]["validFrom"])
                valid_to = int(klass.filtered_regions[region]["validTo"])
                if int(year) in range(valid_from, valid_to):
                    region_index[region] = counter
                    region_label[region] = data["dimension"][data["id"][table_region]]["category"]["label"][region]
                    year_index[year] = 0
                    year_label[year] = year
                    for val in data["value"][region_value_pos:region_value_range:size[table_tid]]:
                        result["value"].append(val)
                            
                    counter += 1
            region_value_pos += region_value_size
            region_value_range += region_value_size
        result["dimension"][data["id"][table_region]]["category"]["index"] = region_index
        result["dimension"][data["id"][table_region]]["category"]["label"] = region_label
        result["dimension"][data["id"][table_tid]]["category"]["index"] = year_index
        result["dimension"][data["id"][table_tid]]["category"]["label"] = year_label
        result["size"][table_region] = counter
        result["size"][table_tid] = 1
        results = pyjstat.from_json_stat(result, naming="id")

        dataframes.extend(results)
    return dataframes


def post_query():
    dataframes = []
    if ssb_table.total_rows < ssb_table.ssb_max_row_query:
        query = build_query()
        data = requests.post(ssb_table.metadata_url, json=query)
        results = data_filter(data.json(object_pairs_hook=OrderedDict))
        
        dataframes = pd.concat(results, ignore_index=True)
    else:
        for i in range(len(ssb_table.dimension_iterate["values"])):
            query = build_query(i)
            data = requests.post(ssb_table.metadata_url, json=query)
            results = data_filter(data.json(object_pairs_hook=OrderedDict))
            dataframes.extend(results)
        big_df = pd.concat(dataframes, ignore_index=True)
        return big_df
    return dataframes

r = post_query()