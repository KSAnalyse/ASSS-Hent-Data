import pandas as pd
from pyjstat import pyjstat
import requests
from collections import OrderedDict
import time
import copy
import re
import json
from datetime import datetime


class SSBTable:
    """ A class used to get metadata from ssb.no, process them and keep track of variables.
    
    ...

    Attributes:
    -----------
    table_id : str
        Table number that's used to query against correct ssb table.
    metadata_filter : list
        A list that's set to None by default, unless a filter has been passed along. 
        This filter defines what data we will query with, if it's empty we will query for everything.

    Methods:
    --------
    metadata_url(self):
        Creates a URL string for the table we will query.
    metadata_variables(self, metadata_filter):
        Does a JSON get request to the table metadata URL.
        Calls on filter_json_metadata to filter the data based on the filters tags.
    filter_json_metadata(self, json_metadata, filter_dict):
        ...
    filter_as_dict
        ...
    find_table_dimensions():
        Calculates the size of the table except for 
        Tid and Region since we will be iterating on those.
    """

    def __init__(self, table_id, metadata_filter=None):
        """
        Parameters:
        -----------
        table_id : str
            Table number that's used to query against correct ssb table.
        metadata_filter : list/None
            A list that's set to None by default, unless a filter has been passed along. 
            This filter defines what data we will query with, if it's empty we will query for everything.
        
        Attributes:
        -----------
        variables : list
            A complete list of the tables metadata, except for what had been filtered out by filter_json_metadata.
        table_region : int
            Position of the Region dimension in variables.
        table_tid : int
            Position of the Tid dimension in variables.
        table_size : int
            Row size of the dimensions, except for Region and Tid.
        ssb_max_row_query : int
            Maximum rows we can query per request to SSB.no
        """
        self.table_id = table_id
        self.metadata_filter = metadata_filter
        self.variables = self.metadata_variables(metadata_filter)
        self.table_region, self.table_tid, self.table_size = self.find_table_dimensions
        self.ssb_max_row_query = 800000

    @property
    def metadata_url(self):
        """ Creates URL based on the table_id

        Returns:
        --------
        url : string
            returns a URL string for the table we will query.
        """
        url = "http://data.ssb.no/api/v0/no/table/" + self.table_id
        return url

    def metadata_variables(self, metadata_filter):
        """ JSON request for the metadata.

        Does a JSON get request for the metadata for the table we will query.
        If a filter is provided it will call the filter_json_metadata() function to filter it
        first then return it.

        Parameters:
        -----------
        metadata_filter : None/string
            Empty by default, unless a filter is provided.

        Returns:
        --------
        filtered_variables : list
            returns the metadata requested.
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
        """Filters out metadata that isn't in the filter string.

        Parameters:
        -----------
        json_metadata : list
            A complete list of the tables metadata, except for what had been filtered out by filter_json_metadata.
        filter_dict : dict
            A dict of the filter string provided by filters_as_dict
        
        Returns:
        --------
        json_metadata : dict
            Returns a filtered dict of the metadata.
        """
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
        """ Splits up the filter string and returns it as a dict.

        Parameters:
        -----------
        filter_string : str
            A string with filter tags.
        
        Returns:
        --------
        filters : dict
            Returns a dict of the filter string.
        """
        filters = {}
        filter_args = re.split("[=&]", filter_string)
        for idx, meta_filter in enumerate(filter_args):
            if ((idx % 2) == 0):
                filters[meta_filter] = filter_args[filter_args.index(
                    meta_filter) + 1].split(",")
        return filters

    @property
    def find_table_dimensions(self):
        """ Calculate size of table, except for Region and Tid.

        Returns:
        --------
        table_region : int
            Position of the Region dimension in variables.
        table_tid : int
            Position of the Tid dimension in variables.
        table_size : int
            Row size of the dimensions, except for Region and Tid.
        """
        table_region = 0
        table_tid = 0
        table_size = 1
        for v_idx, var in enumerate(self.variables["variables"]):
            if var["text"] == "region":
                table_region = v_idx
            elif var["code"] == "Tid":
                table_tid = v_idx            
            else:
                table_size *= len(var["values"])
        return table_region, table_tid, table_size

class RegionKLASS:
    """ A class used to get classification list from SSB to keep track of which regions are valid within the last five years

    This class is primarely used to get a list of regioncodes and their validity within the last five years.
    """

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

def post_query():
    dataframes = []
    meta_data = meta_filter()
    #result_list =[]

    for variables in meta_data:
        query = build_query(variables)
        data = requests.post(ssb_table.metadata_url, json=query)
        #result_list.append(data)
        #print(data)
        time.sleep(2.0)
        results = pyjstat.from_json_stat(data.json(object_pairs_hook=OrderedDict), naming="id")
        dataframes.append(results[0])
    big_df = pd.concat(dataframes, ignore_index=True)
    return big_df
    
ssb_table = SSBTable(TabellNummer, Filter)
klass = RegionKLASS(["131", "104", "214", "231"])
r = post_query()