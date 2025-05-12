# excel_parser.py

import os
import pandas as pd
from pandas.errors import EmptyDataError
from collections.abc import Hashable
from typing import Any, Dict, List, Tuple, Union, Optional
import copy
import numpy as np # Import numpy for np.nan

# --- Data Cleaning (Enhanced) ---
NONE_SET = {
    pd.NA, None, "", " ", "nan", "NaN", "NAN", "Nan", "N/A", "n/a", "N/a",
    "null", "Null", "NULL", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN",
    "-NaN", "-nan", "1.#IND", "1.#QNAN", "<NA>", "NA", "NULL", "NaN", "n/a",
    "nan", "null", pd.NaT, "NaT", str(np.nan) # Add string representation of nan
}
# Explicitly keep np.nan separate for replacement logic before ffill
FFILL_REPLACE_SET = {"", " "} # Values to replace with np.nan before ffill

def get_clean_value(raw_value: Any) -> Any | None:
    """Cleans raw data values, checking against NONE_SET and attempting numeric conversion."""
    if pd.isna(raw_value): # Handles pd.NA, pd.NaT, np.nan
        return None
    # Check string representation against NONE_SET *after* stripping
    val_str = str(raw_value).strip()
    if val_str in NONE_SET:
        return None

    # Attempt numeric conversion for strings that weren't cleaned above
    if isinstance(raw_value, str): # Check original type was string
        # Try converting to int first (most specific)
        try:
            # Check if it looks like a float representation of an integer first
            float_val = float(val_str)
            if float_val.is_integer():
                 # Check precision limits before converting large floats
                 if abs(float_val) < 1e15:
                      return int(float_val)
                 else:
                      # Return the large float representation if too big for precise int
                      return float_val
            # If not an integer float, return the float
            return float_val
        except ValueError:
            # If float conversion fails, it's likely a non-numeric string
            pass # Fall through to return original string value later

    # Handle float to int conversion for non-string inputs
    if isinstance(raw_value, float) and raw_value.is_integer():
        if abs(raw_value) < 1e15:
            return int(raw_value)
        else:
            return raw_value # Return large float as is

    if isinstance(raw_value, str):
        return val_str # Return the stripped string if no conversion happened

    return raw_value # Return original value otherwise

def _form_dict_key_from_groupby(key_tuple: Union[tuple, Any], num_keys: int) -> Hashable | None:
    """Forms a valid dictionary key from groupby results, cleaning parts. Returns None if all parts are None."""
    if not isinstance(key_tuple, tuple):
        key_tuple = (key_tuple,)

    cleaned_key_parts = [get_clean_value(k) for k in key_tuple]

    if num_keys > 1 and all(k is None for k in cleaned_key_parts):
        return None # Invalid multi-key

    # Return single cleaned value if originally one key, else the tuple
    return cleaned_key_parts[0] if num_keys == 1 else tuple(cleaned_key_parts)

def _check_template_validity(template: List[List[Optional[None]]]) -> bool:
    """Validates template: non-empty list of non-empty lists containing only None."""
    if not isinstance(template, list) or not template:
        return False
    for i, level in enumerate(template):
        if not isinstance(level, list) or not level:
            return False
        if not all(item is None for item in level):
            return False
    return True

def _extract_recursive(
    df_slice: pd.DataFrame,
    template: List[List[Optional[None]]]
) -> Union[Dict[Hashable, Any], List[Any], None]:
    """Internal recursive helper to extract data based on remaining template levels."""
    if df_slice.empty or not template:
        return None

    current_level_config = template[0]
    num_cols_this_level = len(current_level_config)

    if df_slice.shape[1] < num_cols_this_level:
        print(f"Warning: Slice has insufficient columns ({df_slice.shape[1]}) for template level ({num_cols_this_level}). Returning None. Slice head:\n{df_slice.head()}")
        return None

    if len(template) == 1:
        leaf_values = []
        leaf_cols = df_slice.columns[:num_cols_this_level]
        for _, row in df_slice[leaf_cols].iterrows():
            row_data = [get_clean_value(row[col]) for col in leaf_cols]
            if all(v is None for v in row_data):
                continue
            leaf_values.append(row_data[0] if num_cols_this_level == 1 else row_data)
        return leaf_values if leaf_values else None

    results: Dict[Hashable, Any] = {}
    key_column_names = df_slice.columns[:num_cols_this_level].tolist()

    try:
        grouped = df_slice.groupby(by=key_column_names, sort=False, dropna=False)
    except Exception as e:
        print(f"Error during recursive groupby on columns {key_column_names}. Slice info:\n{df_slice.info()}\n{df_slice.head()}")
        raise RuntimeError(f"Recursive grouping failed: {e}") from e

    for key_tuple, group_df in grouped:
        dict_key = _form_dict_key_from_groupby(key_tuple, num_cols_this_level)

        next_level_col_indices = list(range(num_cols_this_level, df_slice.shape[1]))
        if not next_level_col_indices:
             print(f"Warning: No columns remaining for recursive call at key '{dict_key}'. Check template depth vs data columns.")
             sub_result = None
        else:
            next_level_col_names = df_slice.columns[next_level_col_indices]
            next_level_df_slice = group_df[next_level_col_names]
            sub_result = _extract_recursive(next_level_df_slice, template[1:])

        if sub_result is not None:
            results[dict_key] = sub_result

    return results if results else None


def extract_data_with_excel_dict(
    excel_path: str,
    template: List[List[Optional[None]]],
    skiprows: Optional[int] = None,
) -> List[Any]:
    """
    Extracts structured data from Excel/CSV based on template, always returning a list.
    (Args, Returns, Raises documentation remains the same)
    """
    # 1. --- Validation Checks Upfront ---
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"File not found: {excel_path}")
    if not _check_template_validity(template):
        raise ValueError("Invalid template structure.")
    total_template_cols = sum(len(level) for level in template)
    if total_template_cols <= 0:
         raise ValueError("Invalid template: requires at least one column.")

    # 2. --- File Reading ---
    file_ext = os.path.splitext(excel_path)[1].lower()
    pd_data = pd.DataFrame()
    actual_skiprows = skiprows if skiprows is not None else 0
    read_args = {'header': None, 'skiprows': actual_skiprows}
    na_values_for_read = list(NONE_SET - {None, '', ' ', pd.NA, pd.NaT}) # Keep '', ' ' as strings initially

    try:
        dtype_settings = object # Read all as object initially
        if file_ext == '.xlsx':
            pd_data = pd.read_excel(excel_path, engine='openpyxl', dtype=dtype_settings, keep_default_na=False, na_values=na_values_for_read, **read_args)
        elif file_ext == '.csv':
             pd_data = pd.read_csv(excel_path, dtype=dtype_settings, keep_default_na=False, na_values=na_values_for_read, **read_args)
        else:
            raise ValueError(f"Unsupported file type: {file_ext}. Provide .xlsx or .csv.")

    except EmptyDataError:
        return []
    except Exception as e:
        if isinstance(e, ValueError) and "Unsupported file type" in str(e):
             raise e
        raise RuntimeError(f"Error reading file '{excel_path}': {str(e)}") from e

    # 3. --- Post-Read Checks ---
    if pd_data.empty:
        return []
    if pd_data.shape[1] < total_template_cols:
         raise ValueError(f"Data column count mismatch: Template requires {total_template_cols}, file '{excel_path}' has {pd_data.shape[1]}.")

    pd_data.columns = range(pd_data.shape[1])

    # 4. --- Data Processing ---
    results_list = []

    # --- Handle Flat Template (Single Level) ---
    if len(template) == 1:
        num_cols = len(template[0])
        target_cols = list(range(num_cols))
        for _, row in pd_data[target_cols].iterrows():
            # Apply cleaning directly for flat data
            row_data = [get_clean_value(row[col]) for col in target_cols]
            if not all(v is None for v in row_data):
                results_list.append(row_data[0] if num_cols == 1 else row_data)

    # --- Handle Hierarchical Template (Multiple Levels) ---
    else:
        key_col_indices = []
        cumulative_cols = 0
        for level_config in template[:-1]:
            num_cols_in_level = len(level_config)
            key_col_indices.extend(range(cumulative_cols, cumulative_cols + num_cols_in_level))
            cumulative_cols += num_cols_in_level

        pd_data_filled = pd_data.copy()
        if key_col_indices:
            for col_idx in key_col_indices:
                 pd_data_filled.loc[:, col_idx] = pd_data_filled.loc[:, col_idx].replace(list(FFILL_REPLACE_SET), np.nan)

            pd_data_filled[key_col_indices] = pd_data_filled[key_col_indices].ffill()

        top_level_config = template[0]
        num_top_level_keys = len(top_level_config)
        top_level_key_cols = list(range(num_top_level_keys))

        try:
            grouped_top_level = pd_data_filled.groupby(by=top_level_key_cols, sort=False, dropna=False)
        except Exception as e:
            print(f"Error during top-level groupby on columns {top_level_key_cols}. Data info:\n{pd_data_filled.info()}\n{pd_data_filled.head()}")
            raise RuntimeError(f"Top-level grouping failed: {e}") from e

        for key_tuple, group_df in grouped_top_level:
            top_level_dict_key = _form_dict_key_from_groupby(key_tuple, num_top_level_keys)

            all_cols = list(range(pd_data_filled.shape[1]))
            cols_for_recursion = [c for c in all_cols if c not in top_level_key_cols]

            if not cols_for_recursion:
                print(f"Warning: No columns remaining for recursion for top-level key '{top_level_dict_key}'. Treating as empty.")
                sub_result = None
            else:
                next_level_df_slice = group_df[cols_for_recursion]
                sub_template = copy.deepcopy(template[1:])
                sub_result = _extract_recursive(next_level_df_slice, sub_template)

            if sub_result is not None:
                 results_list.append({top_level_dict_key: sub_result})


    # 5. --- Return Result ---
    return results_list