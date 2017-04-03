"""Plugin for creating, interacting, and updating datasets in Domo.

Domo
-----
Create datasets in a domo instance.
Fetch datasets from domo for custom analytics and/or parsing.
Update (Replace) Domo datasets
"""

import json
import os.path
import sys
import tempfile

import pandas as pd
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib.oauth2_session import OAuth2Session


class DomoData:
    """Methods for fetching, creating, listing and updating Domo Datasets.

        DataSet APIs
        ------------
        **Fetch**
            Retrieve Dataset to memory as DataFrame
            Retrieve Dataset and save to file
        **Create**
            Create Dataset MetaData and Create in Domo
            Create PDP
        **Update**
            Update DataSet MetaData
            Update DataSet PDP
        **List**
            Get Dataset MetaData
            Get Dataset List
            Get Dataset PDP
            Get Dataset PDP list
        **Push**
            Replace DataSet with POSTed Data
        **Delete**
            Delete a DataSet
            Delete DataSet PDP


        Parameters
        ----------
        - **clientId** : *str*
            Client ID for Domo Auth
        - **clientSecret** : *str*
            Client Secret, keep this safe, treat it like a password!
    """

    _client = None
    _domo = None
    _token = None
    domoHeaders = {'Content-Type': 'text/csv'}
    ds_endpoint = None
    # base url for backwards compatible concat
    # domoDataApiBase = 'https://api.domo.com/v1/datasets/'

    def __init__(self, clientId, clientSecret):
        """Configure Backend Client."""
        self.meta_string = {}
        self.csv_dataset = None
        self.update_endpoint = None
        self.input_is_df = False
        self._set_client(client_id=clientId, client_secret=clientSecret)
        self._get_token(client_id=clientId, client_secret=clientSecret)

    def _set_client(self, client_id):
        """Create Server backend client for Domo API connection."""
        self._client = BackendApplicationClient(client_id=client_id)
        self._domo = OAuth2Session(client=self.client)

    def _get_token(self, client_id, client_secret):
        """Authenticate with Domo API."""
        token_url = 'https://api.domo.com/oauth/token?grant_type=client_credentials&scope=data'
        self._token = self._domo.fetch_token(
            token_url=token_url, client_id=client_id, client_secret=client_secret)

    def fetch_dataset(self, dataset_id):
        """Retrieve Dataset and return a dataframe.

            Parameters
            ----------
            **dataset_id**: *string*
                ID of Domo Dataset. e.g. "4405ff58-1957-45f0-82bd-914d989a3ea3"
        """

        dataset_url = f'https://api.domo.com/v1/datasets/{dataset_id}/data?includeHeader=true'
        # used f-strings, as in fast-strings.
        # Not that I'm practically saving time over
        # backwards compatible concat (below) or .format()
        # just more readable!
        # dataset_url = self.domoDataApiBase + dataset_id + '/data?includeHeader=true'

        pulled_ds = self._domo.get(dataset_url)
        temp = tempfile.TemporaryFile(mode='r+')
        temp.write(pulled_ds.text)
        temp.seek(0)
        dframe = pd.read_csv(temp)
        temp.close()
        return dframe

    def fetch_to_file(self, dataset_id, path_to_save, absolute_path=False):
        """Retrieve Dataset and save to file.

            Parameters
            ----------
            - **dataset_id**: *str*
                ID of Domo Dataset. e.g. "4405ff58-1957-45f0-82bd-914d989a3ea3"
            - **path_to_save**: *str*
                path to save csv of dataset to
            - **absolute_path**: *bool*
                Whether to interpret path_to_save as absolute_path or relative to __file__
                defaults to relative path
        """
        dataset_url = f'https://api.domo.com/v1/datasets/{dataset_id}/data?includeHeader=true'
        # another 3.6+ line of code above
        # dataset_url = self.domoDataApiBase + dataset_id + '/data?includeHeader=true'
        dataset_request = self._domo.get(dataset_url)
        if dataset_request.status_code is 200:
            if absolute_path:
                filepath_to_write = path_to_save
            else:
                filepath_to_write = os.path.join(os.path.dirname(
                    os.path.realpath(sys.argv[0])), path_to_save)
                with open(filepath_to_write, 'w') as dataset_file:
                    dataset_file.write(dataset_request.text)
        else:
            print(
                f'Failed to retrieve dataset. {dataset_request.status_code} {dataset_request.reason}')

    def create_ds(self, ds_name, ds_descr, data=None, dataframe=None, col_dtypes_dict=None):
        """Create dataset and push data.

            Parameters
            ----------
            - **ds_name**: *str*
                name for Domo DataSet.
            - **ds_descr**: *str*
                description for Domo DataSet.
            - **data**: *dict*
                data to infer schema from.
            - **date_parse**: *bool*
                increase amount of fields infered as date/datetime.
        """
        meta_json_dict = {}
        if col_dtypes_dict is not None:
            meta_json_dict = self.create_meta_string_from_user_declared(
                ds_name, ds_descr, col_dtypes_dict)
        elif dataframe is not None:
            meta_json_dict = self.create_meta_string_from_dataframe(
                ds_name, ds_descr, dataframe)
        else:
            pass

        url = 'https://api.domo.com/v1/datasets/'
        create_ds = self._domo.post(url=url, json=self.meta_string)
        if create_ds.status_code is 201:
            print("Dataset Created")
            # return response body?
        else:
            print(
                f"Dataset creation failed. {create_ds.status_code} {create_ds.reason}")

    def update_dataset_meta(self, ds_name, ds_desc, data=None, date_parse=False):
        """Update Domo Dataset Schema.

            Parameters
            ----------
            - **ds_name**: *str*
                name for Domo DataSet.
            - **ds_descr**: *str*
                description for Domo DataSet.
            - **data**: *dict*
                data to infer schema from.
            - **date_parse**: *bool*
                increase amount of fields infered as date/datetime.
        """
        self.get_dataset_id()
        self.create_meta_string(ds_name, ds_desc, data, date_parse)
        update_meta_request = self._domo.put(
            self.update_endpoint, json=self.meta_string)
        if update_meta_request.status_code is 200:
            print("Metadata Updated")
        else:
            print("Metadata Update Failed")

    def replace_ds(self, ds_name, ds_descr, data=None, date_parse=False):
        """Update (replace) a dataset in Domo or create if it doesn't already exist.

            Parameters
            ----------
            - **ds_name**: *str*
                name for Domo DataSet.
            - **ds_descr**: *str*
                description for Domo DataSet.
            - **data**: *dict*
                data to infer schema from.
            - **date_parse**: *bool*
                increase amount of fields infered as date/datetime.
        """
        self.get_dataset_id()
        self.update_dataset_meta(ds_name, ds_descr, data, date_parse)
        if self.ds_endpoint != None:
            data_push = self._domo.put(
                self.ds_endpoint, data=self.csv_dataset, headers=self.domoHeaders)
            if data_push.status_code == 204:
                print("Replace Sucessful")
            else:
                print(
                    f'Update Failed. {data_push.status_code} {data_push.reason}')
        else:
            print("No Endpoint Configured for Dataset")

    def list_datasets(self, limit=50, offset=0, sort="name"):
        """Query Domo API for list of Datasets, returns dict upon success.

            Parameters
            ----------
            - **limit**: *int*
                Number of datasets to return. 
                limit=0 returns all datasets
            - **offset**: *int*
                Starting number to retrieve
            - **sort**: *str*
                DataSet field to sort by.
                Prefixing with a negative sign reverses sort order.
        """
        url = f'https://api.domo.com/v1/datasets?limit={limit}&offset={offset}&sort={sort}'
        ds_list = self._domo.get(url=url)
        if ds_list.status_code is 200:
            datasets = json.loads(ds_list.text)
            return datasets
        else:
            print(f'Query Failed. {ds_list.status_code}')
            #print("Query Failed")

    def delete_dataset(self, dataset_id):
        """Delete a specified dataset in Domo.

            Parameters
            ----------
            **dataset_id**: *str*
                ID of Domo DataSet to delete. e.g. "4405ff58-1957-45f0-82bd-914d989a3ea3"
        """
        url = f'https://api.domo.com/v1/datasets/{dataset_id}'
        # or backwards compatible way...
        # url = self.domoDataApiBase + dataset_id

        deleteDataSet = self._domo.delete(url=url)
        if deleteDataSet.status_code is 204:
            print("DataSet Deleted")
        else:
            print(
                f'DataSet Failed to be removed. {deleteDataSet.status_code} {deleteDataSet.reason}')
            #print("DataSet Failed to be removed")

    def create_pdp(self, dataset_id, policy_name, user_ids, group_ids, filters, type):
        """Create PDP policy on specified dataset."""
        url = f'https://api.domo.com/v1/datasets/{dataset_id}/policies'
        # TODO: Create PDP JSON from kwargs
        pdp = "TODO"
        add_pdp = self._domo.post(url=url, json=pdp)
        if add_pdp.status_code is 201:
            print("PDP Sucessfully Created")
            # maybe return body context? text or dict?
            # return add_pdp.text
        else:
            print("PDP Creation Failed ", add_pdp.status_code)

    def get_pdp(self, dataset_id, pdp_id=None):
        """Get all PDP's (optionally single PDP by pdp_id) for a given dataset."""
        url = f'https://api.domo.com/v1/datasets/{dataset_id}/policies/{pdp_id}'
        retrieve_pdp = self._domo.get(url=url)
        if retrieve_pdp.status_code is 200:
            # return content body or parse to dict and return dict?
            return retrieve_pdp.text
        else:
            print("PDP Query failed", retrieve_pdp.status_code)

    def update_pdp(self, dataset_id, pdp_id, policy_name, user_ids, group_ids, filters, type):
        """Update an existing PDP on a given dataset."""
        url = f'https://api.domo.com/v1/datasets/{dataset_id}/policies/{pdp_id}'
        # TODO: Create PDP JSON from kwargs
        # Thought: maybe retrieve the PDP first to use to
        # make kwargs only necessary for changes?
        pdp = "TODO"
        put_pdp = self._domo.put(url=url, json=pdp)

        if put_pdp.status_code is 200:
            print("PDP Update Successful")
            # return a response? Content Body text or
            # parsed to dict?
            # return put_pdp.text

    def delete_pdp(self, dataset_id, pdp_id):
        """Delete an existing PDP on a given dataset."""
        url = f'https://api.domo.com/v1/datasets/{dataset_id}/policies/{pdp_id}'
        remove_pdp = self._domo.delete(url=url)

        if remove_pdp is 204:
            print(f'PDP {pdp_id} sucessfully removed')
        else:
            print(
                f'Failed to delete PDP {pdp_id}. {remove_pdp.status_code} {remove_pdp.reason}')

    def get_dataset_id(self):
        """Get dataset Id or create dataset if it doesn't exist in Domo."""
        # TODO: Query Dataset List, Search for Dataset
        pass

    def create_meta_string_from_dataframe(self, ds_name, ds_descr, dataframe):
        """Create Columns type, name list for Domo Schema based on Dataframe dtypes."""
        df_domo_types = {'object': "STRING", 'int64': "LONG", 'int32': "LONG",
                         'float64': "DOUBLE", 'float32': "DOUBLE", 'float': "DOUBLE",
                         'datetime64[ns]': "DATE", 'datetime64[ns, tz]': "DATETIME",
                         'bool': "STRING"}

        columns = []
        for col in dataframe.columns:
            col_type = df_domo_types.get(str(dataframe[col].dtype), "STRING")
            columns.append({"type": col_type, "name": col})

        meta_json_dict = {}
        meta_json_dict['name'] = ds_name
        meta_json_dict['description'] = ds_descr
        meta_json_dict['rows'] = 0
        meta_json_dict['schema'] = {"columns": columns}

        return meta_json_dict

    def create_meta_string_from_user_declared(self, ds_name, ds_descr, col_types_dict):
        """Create metadata from user declared dict of key/value column/type pairs."""
        # set of allowed data types for Domo Metadata
        valid_domo_types = ["STRING", "DOUBLE", "DATE", "DATETIME", "LONG"]
        #valid_domo_types = set(domo_types)
        # types user passed in
        types_for_dataset = set(col_types_dict.values())
        # make sure user passed in types are valid Domo Metadata types.
        for dtype in types_for_dataset:
            if dtype not in valid_domo_types:
                raise ValueError(
                    f"{dtype} is not a valid datatype to use for creating a Domo Dataset")
        # Create dict of meta for json req.
        meta_json_dict = {}
        meta_json_dict['name'] = ds_name
        meta_json_dict['description'] = ds_descr
        meta_json_dict['rows'] = 0
        meta_json_dict['schema'] = {"columns": [
            {"name": k, "type": v} for k, v in col_types_dict.items()]}

        return meta_json_dict

    def create_meta_string_from_json_data():
        """Create metadata with data types infered from json."""
        pass

    def create_pdp_req(self, pdp_name, filters, groups=[], users=[]):
        """Create pdp json req body."""
        # Should check that filter ops passed are valid. Maybe make filter generator def.
        # filter_operators = ["EQUALS", "LIKE", "GREATER_THAN", "LESS_THAN", "GREATER_THAN_EQUAL",
        #                     "LESS_THAN_EQUAL", "BETWEEN", "BEGINS_WITH", "ENDS_WITH", "CONTAINS"]

        pdp_json_dict = {}
        pdp_json_dict['name'] = pdp_name
        pdp_json_dict['filters'] = filters
        pdp_json_dict['users'] = users
        if groups != []:
            pdp_json_dict['groups'] = groups

        return pdp_json_dict

    def set_dataset_endpoint(self, dataset_id):
        """Configure enpoint to use for fetching or updating a dataset."""
        self.ds_endpoint = self.domoDataApiBase + dataset_id + '/data'
        self.update_endpoint = self.domoDataApiBase + dataset_id

    def set_dataframe(self, dataframe):
        """Set df to Upload to Domo."""
        self.df = dataframe
        self.csv_dataset = dataframe.to_csv(index=False, header=False)
        self.input_is_df = True
