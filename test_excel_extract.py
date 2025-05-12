import unittest
import os
import pandas as pd
import numpy as np  # For creating NaN test values
from typing import List, Any

from excel_extract import extract_data_with_excel_dict

# --- Helper function to create dummy files for testing ---
def create_dummy_file(filepath_base: str, data: List[List[Any]], file_type: str = 'excel') -> str:
    """Creates a dummy excel or csv file without header/index."""
    df = pd.DataFrame(data)
    if file_type.lower() == 'excel':
        filepath = filepath_base + ".xlsx"
        # Use openpyxl engine explicitly for consistency if needed
        df.to_excel(filepath, index=False, header=False, engine='openpyxl')
    elif file_type.lower() == 'csv':
        filepath = filepath_base + ".csv"
        df.to_csv(filepath, index=False, header=False)
    else:
        raise ValueError("Unsupported file_type for dummy file creation")
    # print(f"Created dummy file: {filepath}") # Keep commented unless debugging setup
    return filepath

# --- Test Class ---
class TestExtractDataBlackBox(unittest.TestCase):

    def setUp(self):
        """Create a list to track dummy files created during tests."""
        self.test_files: List[str] = []

    def tearDown(self):
        """Remove all dummy files created during a test."""
        for f in self.test_files:
            if os.path.exists(f):
                try:
                    os.remove(f)
                    # print(f"Removed dummy file: {f}") # Keep commented unless debugging setup
                except OSError as e:
                    print(f"Warning: Error removing file {f}: {e}")

    def _create_and_track(self, filename_base: str, data: List[List[Any]], file_type: str = 'excel') -> str:
        """Helper to create a dummy file and register it for cleanup."""
        filepath = create_dummy_file(filename_base, data, file_type)
        self.test_files.append(filepath)
        return filepath

    # --- Basic Functionality Tests ---

    def test_flat_data_excel(self):
        """Scenario: Simple flat data from an Excel file."""
        data = [[1, "Apple", 100], [2, "Banana", ""], [3, "Orange", pd.NA]]
        template = [[None, None, None]]
        filepath = self._create_and_track("test_flat", data, 'excel')
        expected = [[1, "Apple", 100], [2, "Banana", None], [3, "Orange", None]]
        result = extract_data_with_excel_dict(filepath, template)
        self.assertEqual(result, expected, "Flat Excel data extraction failed.")

    def test_flat_data_csv(self):
        """Scenario: Simple flat data from a CSV file."""
        data = [[10.0, "X"], [20, "Y"], [30, None]] # Test float int conversion too
        template = [[None, None]]
        filepath = self._create_and_track("test_flat_csv", data, 'csv')
        expected = [[10, "X"], [20, "Y"], [30, None]]
        result = extract_data_with_excel_dict(filepath, template)
        self.assertEqual(result, expected, "Flat CSV data extraction failed.")

    def test_flat_data_single_column(self):
        """Scenario: Flat data, single column extraction."""
        data = [["A"], ["B"], [None], ["C"], [""]]
        template = [[None]]
        filepath = self._create_and_track("test_flat_single", data, 'csv')
        # Assuming implementation skips rows where all values become None
        expected = ["A", "B", "C"]
        result = extract_data_with_excel_dict(filepath, template)
        self.assertEqual(result, expected, "Single column flat data extraction failed.")

    def test_hierarchical_simple_2levels(self):
        """Scenario: Simple 2-level hierarchy (Key -> List of values)."""
        data = [["H1", "A"], ["", "B"], ["H2", "C"], ["", "D"]] # Added 'D' under H2
        template = [[None], [None]]
        filepath = self._create_and_track("test_hier_2lvl", data, 'excel')
        expected = [{'H1': ['A', 'B']}, {'H2': ['C', 'D']}]
        result = extract_data_with_excel_dict(filepath, template)
        self.assertEqual(result, expected, "Simple 2-level hierarchy failed.")

    def test_hierarchical_simple_3levels(self):
        """Scenario: Simple 3-level hierarchy (Key -> SubKey -> List of values)."""
        data = [["R1", "S1", "V1"], ["", "S2", "V2"], ["R2", "S3", "V3a"], ["R2", "S3", "V3b"]]
        template = [[None], [None], [None]]
        filepath = self._create_and_track("test_hier_3lvl", data, 'excel')
        expected = [
            {'R1': {'S1': ['V1'], 'S2': ['V2']}},
            {'R2': {'S3': ['V3a', 'V3b']}}
        ]
        result = extract_data_with_excel_dict(filepath, template)
        self.assertEqual(result, expected, "Simple 3-level hierarchy failed.")

    def test_hierarchical_multi_key_level(self):
        """Scenario: Hierarchy with a multi-column key level."""
        data = [["G1", "K1", "V1"], ["", "K1", "V2"], ["G1", "K2", "V3"], ["G2", "K1", "V4"]]
        template = [[None, None], [None]] # Level 1 key: (Col 0, Col 1)
        filepath = self._create_and_track("test_hier_multikey", data, 'excel')
        expected = [
            {('G1', 'K1'): ['V1', 'V2']},
            {('G1', 'K2'): ['V3']},
            {('G2', 'K1'): ['V4']}
        ]
        result = extract_data_with_excel_dict(filepath, template)
        self.assertEqual(result, expected, "Multi-key hierarchy failed.")

    # --- Data Variation Tests ---

    def test_various_none_values_cleaned(self):
        """Scenario: Various standard 'None' representations are cleaned."""
        data = [
            ["KeyA", " ", 1],    # Whitespace string -> None
            ["KeyA", "", 2],     # Empty string -> None
            ["KeyA", None, 3],   # Python None -> None
            ["KeyA", np.nan, 4], # Numpy NaN -> None
            ["KeyA", pd.NA, 5],  # Pandas NA -> None
            ["KeyA", "N/A", 6],  # Common NA string -> None
            ["KeyA", "NaN", 7],  # String "NaN" -> None
            ["KeyB", "Valid", 8] # Stays valid
        ]
        template = [[None], [None], [None]] # Key / SubKey / Value
        filepath = self._create_and_track("test_nones", data, 'csv')
        expected = [
             {'KeyA': {None: [1, 2, 3, 4, 5, 6, 7]}}, # All cleaned SubKeys map to None
             {'KeyB': {'Valid': [8]}}
        ]
        result = extract_data_with_excel_dict(filepath, template)
        self.assertEqual(result, expected, "Cleaning of various None types failed.")

    def test_numeric_float_integer_conversion(self):
        """Scenario: Floats representing integers are converted to int."""
        data = [[1.0, "Val1"], [2.0, "Val2"], [3.5, "Val3"], [4000000000000000.0, "LargeInt"]]
        template = [[None], [None]]
        filepath = self._create_and_track("test_float_int", data, 'excel')
        expected = [{1: ["Val1"]}, {2: ["Val2"]}, {3.5: ["Val3"]}, {4000000000000000: ["LargeInt"]}]
        result = extract_data_with_excel_dict(filepath, template)
        self.assertEqual(result, expected, "Float-to-int conversion failed.")

    def test_skip_rows_all_none_after_clean(self):
        """Scenario: Rows becoming entirely None after cleaning are skipped (flat)."""
        data = [[1, "A", 10], ["", None, pd.NA], [" ", "N/A", np.nan], [3, "C", 30]]
        template = [[None, None, None]]
        filepath = self._create_and_track("test_all_none_row", data, 'csv')
        expected = [[1, "A", 10], [3, "C", 30]]
        result = extract_data_with_excel_dict(filepath, template)
        self.assertEqual(result, expected, "Skipping all-None rows failed.")

    def test_keys_become_none_grouped(self):
        """Scenario: Hierarchy keys cleaning to None are grouped under None."""
        data = [["N/A", "A", 1], ["", "B", 2], ["", "A", 1.5], ["Key2", "C", 3]]
        template = [[None], [None], [None]]
        filepath = self._create_and_track("test_key_becomes_none", data, 'excel')
        # Assuming the implementation groups keys that clean to None under a single None key
        expected = [{None: {'A': [1, 1.5], 'B': [2]}}, {'Key2': {'C': [3]}}]
        result = extract_data_with_excel_dict(filepath, template)
        self.assertEqual(result, expected, "Grouping under None key failed.")

    # --- Template Variation Tests ---

    def test_template_uses_fewer_columns_than_data(self):
        """Scenario: Template uses fewer columns than available in the data file."""
        data = [["R1", "S1", "V1", "Extra1", "Extra2"], ["", "S2", "V2", "Extra3", "Extra4"]]
        template = [[None], [None], [None]] # Should only process first 3 columns
        filepath = self._create_and_track("test_template_shallow", data, 'excel')
        expected = [{'R1': {'S1': ['V1'], 'S2': ['V2']}}]
        result = extract_data_with_excel_dict(filepath, template)
        self.assertEqual(result, expected, "Ignoring extra data columns failed.")

    # --- File Handling and `skiprows` Tests ---

    def test_empty_file_returns_empty_list(self):
        """Scenario: Input file is completely empty."""
        data = []
        template = [[None]]
        filepath = self._create_and_track("test_empty", data, 'csv')
        expected = []
        result = extract_data_with_excel_dict(filepath, template)
        self.assertEqual(result, expected, "Empty file did not return empty list.")

    def test_skiprows_ignores_initial_rows(self):
        """Scenario: skiprows parameter correctly ignores header/junk rows."""
        data = [["Header1", "Header2"], ["SubHeader", ""], [1, "A"], [2, "B"]]
        template = [[None], [None]]
        filepath = self._create_and_track("test_skiprows", data, 'excel')
        expected = [{1: ["A"]}, {2: ["B"]}]
        result = extract_data_with_excel_dict(filepath, template, skiprows=2)
        self.assertEqual(result, expected, "skiprows basic functionality failed.")

    def test_skiprows_greater_than_rows_returns_empty(self):
        """Scenario: skiprows is larger than the number of rows in the file."""
        data = [[1, "A"], [2, "B"]]
        template = [[None], [None]]
        filepath = self._create_and_track("test_skiprows_empty", data, 'excel')
        expected = []
        result = extract_data_with_excel_dict(filepath, template, skiprows=5)
        self.assertEqual(result, expected, "skiprows > file rows did not return empty list.")

    # --- Error Condition Tests (Expecting Exceptions) ---

    def test_error_file_not_found(self):
        """Scenario: Input file path does not exist, raises FileNotFoundError."""
        template = [[None]]
        # Use assertRaisesRegex for more specific error message checking if desired
        with self.assertRaises(FileNotFoundError, msg="FileNotFoundError not raised for non-existent file."):
            extract_data_with_excel_dict("non_existent_file_xyz.xlsx", template)

    def test_error_invalid_template_structure_not_list(self):
        """Scenario: Template is not a list, raises ValueError."""
        filepath = self._create_and_track("test_invalid_tmpl_1", [["A"]], 'csv')
        # Pass a dictionary instead of list for template
        with self.assertRaisesRegex(ValueError, "Invalid template structure", msg="ValueError not raised for non-list template."):
            extract_data_with_excel_dict(filepath, {"template": "invalid"})

    def test_error_invalid_template_structure_empty_list(self):
        """Scenario: Template is an empty list, raises ValueError."""
        filepath = self._create_and_track("test_invalid_tmpl_2", [["A"]], 'csv')
        with self.assertRaisesRegex(ValueError, "Invalid template structure", msg="ValueError not raised for empty list template."):
            extract_data_with_excel_dict(filepath, [])

    def test_error_invalid_template_structure_inner_not_list(self):
        """Scenario: Template list contains non-list elements, raises ValueError."""
        filepath = self._create_and_track("test_invalid_tmpl_3", [["A"]], 'csv')
        with self.assertRaisesRegex(ValueError, "Invalid template structure", msg="ValueError not raised for template with non-list element."):
            extract_data_with_excel_dict(filepath, [[None], "level2_is_string"])

    def test_error_invalid_template_structure_inner_not_none(self):
        """Scenario: Template inner list contains non-None elements, raises ValueError."""
        filepath = self._create_and_track("test_invalid_tmpl_4", [["A"]], 'csv')
        with self.assertRaisesRegex(ValueError, "Invalid template structure", msg="ValueError not raised for template with non-None placeholder."):
            extract_data_with_excel_dict(filepath, [[None], ["Key1"]])

    def test_error_insufficient_columns_in_data(self):
        """Scenario: Data file has fewer columns than required by the template, raises ValueError."""
        data = [[1, "A"], [2, "B"]] # Only 2 columns
        template = [[None], [None], [None]] # Requires 3 columns
        filepath = self._create_and_track("test_insufficient_cols", data, 'csv')
        # Check for a specific part of the expected error message
        with self.assertRaisesRegex(ValueError, "Data column count mismatch", msg="ValueError not raised for insufficient data columns."):
            extract_data_with_excel_dict(filepath, template)

    def test_error_unsupported_file_type(self):
        """Scenario: File extension is not .xlsx or .csv, raises ValueError after attempting CSV read."""
        # Create a dummy file with a different extension
        filepath_base = "test_unsupported"
        filepath = filepath_base + ".txt"
        with open(filepath, "w") as f:
            f.write("col1,col2\nval1,val2")
        self.test_files.append(filepath) # Ensure cleanup

        template = [[None], [None]]
        with self.assertRaisesRegex(ValueError, "Unsupported file type", msg="ValueError not raised for unsupported file extension."):
             extract_data_with_excel_dict(filepath, template)


# --- Runner ---
if __name__ == '__main__':
    unittest.main(verbosity=2) # verbosity=2 provides more detailed output