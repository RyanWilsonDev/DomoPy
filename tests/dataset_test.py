"""Unit tests for Dataset API."""
import unittest

from domopy import DomoData


class DatasetApiTests(unittest.TestCase):
    """Tests for dataset methods that aren't simply requests fxns."""

    def test_create_ds_metadata(self):
        """Test json req of metadata for dataset creation is properly created."""
        json_sample = {"name": "Leonhard Euler Party", "description": "Mathematician Guest List", "rows": 0,
                       "schema": {
                           "columns": [{
                               "type": "STRING",
                               "name": "Friend"
                           }, {
                               "type": "STRING",
                               "name": "Attending"
                           }]
                       }
                       }
        ds_name = "Leonhard Euler Party"
        ds_description = "Mathematician Guest List"
        col_dtypes_dict = {"Friend": "STRING", "Attending": "STRING"}
        json_generated = DomoData.create_meta_string_from_user_declared(self,
                                                                        ds_name=ds_name, ds_descr=ds_description, col_types_dict=col_dtypes_dict)
        self.assertEqual(json_sample, json_generated)

    def test_create_pdp(self):
        """Test json req for pdp is properly created."""
        json_sample = {
            "name": "Only Show Attendees",
            "filters": [{
                "column": "Attending",
                "values": ["TRUE"],
                "operator": "EQUALS"
            }],
            "users": [27]
        }

        pdp_name = "Only Show Attendees"
        users = [27]
        filter_list = [("Attending", ["TRUE"], "EQUALS")]
        filters = [{"column": a, "values": b, "operator": c}
                   for a, b, c in filter_list]
        json_generated = DomoData.create_pdp_req(
            self, pdp_name=pdp_name, users=users, filters=filters)
        self.assertEqual(json_sample, json_generated)


if __name__ == "__main__":
    unittest.main()
