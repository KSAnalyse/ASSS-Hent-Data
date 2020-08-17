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
        Table number thats used to query against correct ssb table.
    metadata_filter : list
        A list thats set to None by default, unless a filter has been passed along. 
        This filter defines what data we will query with, if its empty we will query for everything.

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
            Table number thats used to query against correct ssb table.
        metadata_filter : list/None
            A list thats set to None by default, unless a filter has been passed along. 
            This filter defines what data we will query with, if its empty we will query for everything.
        
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
        self.table_region, self.table_tid, self.table_size, self.table_total_size = self.find_table_dimensions
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
        """Filters out metadata that isnt in the filter string.

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
                    try:
                        index_of_value = json_metadata["variables"][idx]["values"].index(
                            value)
                        value_texts.append(
                            json_metadata["variables"][idx]["valueTexts"][index_of_value])
                    except ValueError:
                        filter_dict[var["code"]].remove(value)
                        print(value, "finnes ikke i metadata, har blitt fjernet fra spørringen.")
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
        table_region = None
        table_tid = None
        table_size = 1
        table_total_size = 1
        for v_idx, var in enumerate(self.variables["variables"]):
            table_total_size *= len(var["values"])
            if var["text"] == "region":
                table_region = v_idx
            elif var["code"] == "Tid":
                table_tid = v_idx
            else:
                table_size *= len(var["values"])
        return table_region, table_tid, table_size, table_total_size

class RegionKLASS:
    """ A class used to get classification list from SSB to keep track of which regions are valid within the last five years

    This class is primarely used to get a list of region codes and their validity within the last five years.
    Its used to get that list from various classification lists, append them all together, filter out equal ones 
    and merge the ones who only has had a name change and not region code change.

    Attributes:
    -----------
    klass_id : list
        List of classificationcode we are using to get our complete list of region codes.

    Methods:
    --------
    region_klass_url(i):
        Concatenates klass_id with from date to max date from ssb to create the url
    get_klass_variables():
        Does a JSON get request for the classification ID provided and appends it to a list
    filter_klass_variables():
        Prunes the classification code region list to only include code, validfrom and validto dates.
    filter_regions():
        Filters equal codes, merges ones with name change and not region code change.
    """

    def __init__(self, klass_id):
        """
        Parameters:
        -----------
        klass_id : list
            List of classificationcode we are using to get our complete list of region codes.
        
        Attributes:
        -----------
        klass_id : list
            List of classificationcode we are using to get our complete list of region codes.
        to_date : int
            Current year
        from_date : int
            Current year subtracted by five, as we just get data for the past five years.
        klass_variables : list
            List of all the classifications
        filtered_klass_variables : list
            Pruned and filtered list of classifications
        filtered_regions : dict
            Filtered and merged regions. 
        """
        self.klass_id = klass_id
        self.to_date = time.localtime(time.time()).tm_year
        self.from_date = self.to_date - 5
        self.klass_variables = self.get_klass_variables()
        self.filtered_klass_variables = self.filter_klass_variables()
        self.filtered_regions = self.filter_regions()

    def region_klass_url(self, i):
        """ Concatenates klass_id with from date to max date from ssb to create the url

        Parameters:
        -----------
        i : str
            str of the klass_id

        Returns:
        url : str
            The concatenated url
        """
        url = "http://data.ssb.no/api/klass/v1/classifications/" + i + "/codes?from=" + \
            str(self.from_date) + "-01-01&to=2059-01-01&includeFuture=true"
        return url

    def get_klass_variables(self):
        """ Does a JSON get request for the classification ID provided and appends it to a list

        Set a headers dict first, this is so that we get a JSON back. Standard return from SSB is XML.
        Then we loop over the klass_id list and do a get request for each klass_id provided and append them to 
        a list.

        Returns:
        --------
        all_klass_data : list
            Returns a list of all the classification codes.
        """
        all_klass_data = []
        headers = {"Accept": "application/json", "charset": "UTF-8"}
        for i in self.klass_id:
            response = requests.get(self.region_klass_url(i), headers=headers)
            data = response.text

            if ("?" in response.text):
                data = response.text.replace("?", "š")
            all_klass_data.append(data)
        return all_klass_data

    def filter_klass_variables(self):
        """ Prunes the classification code region list to only include code, validfrom and validto dates.

        Runs through the klass_variables list and loads them as a JSON object. It then runs through the 
        JSON object and prunes away everything we dont need so that we only have region code, validfrom and validto.
        This might be an unnecessary step, but it doesnt take much time and is done only once per table.

        Returns:
        --------
        regioner : list
            A list of all the region codes and their valid from/to date.
        """
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
        """ Filters equal codes, merges ones with name change and not region code change.

        In this method we go through the filtered_klass_variables and merge regions that has only changed name, 
        we also filter out regions that are equal.

        Returns:
        --------
        filtered_regions_klass : dict
            A complete list of all region codes we use.
        """
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
    """ A function to build a standard query for the SSB API.

    We set up a standard query as a dict and an empty query list.
    Then it loops over the variables parameter, which is a list of the metadata
    that has been filtered for the regions that are invalid within the last five years.
    It ignores the other values, except for the code, filter and values from the metadata 
    as SSB doesnt use those when querying.
    At the end it appends it query list in the main query dict and returns it.

    Parameters:
    -----------
    variables : list
        Filtered list that has been pruned for regions that are not valid
    _filter : str
        A string parameter for the query filter variable

    Returns:
    --------
    query : dict
        Returns a query for a single year or one that equals 800k rows.

    """
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
        query_details["selection"]["values"].extend(var["values"])
        query["query"].append(query_details)
    return query

def meta_filter():
    """ A function that filters away the regions that are invalid for the past five years.

    We run a double for loop, where the first one iterates on year and the second one goes through regions.
    Its done this way becaue of the way JSON-Stat files are built up, if we dont do a filter and query for each year 
    separately we will end up getting values for regions that are invalid for that year (In SSBs case they 
    are returned as the number 0). For each region we check it against our classification list to check if the 
    region code we are on is valid for the current year we are iterating over. If not, it excludes it from the filter. 
    We also constantly check if the next region added is going to make us surpass 800k rows on a query, is so, it appends 
    the current list to metadata_filter and starts building up a new list from where it left off. If it never reaches 800k 
    per year, it will append the list to metadata_filter when the region loop is done.

    Returns:
    --------
    metadata_filter : list
        A list of the metadata_variables that has been filtered for non valid regions for the past five years. 
    """
    metadata_filter = []
    
    if ssb_table.table_region != None:
        for year in ssb_table.variables["variables"][ssb_table.table_tid]["values"][-1:-6:-1]:
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
                            new_meta_var = copy.deepcopy(ssb_table.variables["variables"])
            new_meta_var[ssb_table.table_region]["values"] = new_meta_regions
            new_meta_var[ssb_table.table_tid]["values"] = [year]
            metadata_filter.append(new_meta_var)
    else:
        if ssb_table.table_total_size < ssb_table.ssb_max_row_query:
            metadata_filter.append(ssb_table.variables["variables"])

    return metadata_filter

def post_query():
    """ A function to do a post query on the SSB API.

    This function does a post query on the SSB API, following the SSB API Documentation, by 
    doing a post request with the query we have built up, we get a JSON stat file back with the result.
    First we run meta_filter() once to get the filtered metadata variables, then for each dict in the list 
    we run the build_query() function and post that query to the SSB API. Which after running that query 
    returns a JSON-Stat file back with the results. We then run that JSON-Stat through pyjstat which converts 
    and structures that file to a pandas DataFrame which gets appended to dataframes list. Once the for loop 
    has finished we run a pandas concat on the dataframes list to convert to one single DF.

    Returns:
    --------
    big_df : Series
        This is the DataFrame that will be returned to the SQL server we are using.
    """

    dataframes = []
    meta_data = meta_filter()
    print(len(meta_data))

    for variables in meta_data:
        query = build_query(variables)
        data = requests.post(ssb_table.metadata_url, json=query)
        if data.status_code != 200:
            print("Feil! Status kode:", data.status_code)
        time.sleep(5.0)
        results = pyjstat.from_json_stat(data.json(object_pairs_hook=OrderedDict), naming="id")
        dataframes.append(results[0])
    big_df = pd.concat(dataframes, ignore_index=True)
    return big_df

ssb_table = SSBTable("07459", "Tid=2016,2017,2018,2019,2020")
klass = RegionKLASS(["131", "104", "214", "231"])
r = post_query()
