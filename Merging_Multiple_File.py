#!/usr/bin/env python
# coding: utf-8

# In[1]:


import streamlit as st
import pandas as pd
from io import BytesIO

# -------------------------
# Page Configuration
# -------------------------
st.set_page_config(page_title="Multi-File Smart Merger", layout="centered")
st.title("Multi-File Merger & Cleaner")


# -------------------------
# Functions
# -------------------------
def load_file(file):
    try:
        if file.size == 0:
            raise ValueError("File is empty")
        df = pd.read_csv(file) if file.name.endswith(".csv") else pd.read_excel(file)
        if df.empty:
            raise ValueError("No data found")
        df.columns = df.columns.str.lower().str.strip()
        return df
    except Exception as e:
        st.error(f"Failed to load {file.name}: {e}")
        return None

def clean_data(df, file_name):
    if df is None:
        st.warning(f"Skipping cleaning for {file_name}: file could not be loaded.")
        return pd.DataFrame()
    if "completion %" in df.columns:
        original_shape = df.shape
        df = df.dropna(subset=["completion %"])
        st.info(f"Cleaned {file_name} Removed rows with 'N/A' in completion %: {original_shape[0] - df.shape[0]}")
    return df

def merge_datasets(base_df, new_df, key_base, key_new):
    try:
        base_df[key_base] = base_df[key_base].astype(str).str.strip()
        new_df[key_new] = new_df[key_new].astype(str).str.strip()
        new_df = new_df.dropna(subset=[key_new])

        # Check for duplicates in the secondary key column
        duplicate_keys = new_df[key_new][new_df[key_new].duplicated()].unique()
        if len(duplicate_keys) > 0:
            st.error(f"Duplicate key values found in the secondary file: {', '.join(duplicate_keys[:10])}")
            st.stop()

        merged = pd.merge(
            base_df,
            new_df,
            left_on=key_base,
            right_on=key_new,
            how='left',
            suffixes=('', f'_{key_new}_merge')
        )
        return merged
    except Exception as e:
        st.error(f"Merge failed: {e}")
        return base_df

def download_csv(df, filename="Merged_Input_Data_File.csv"):
    towrite = BytesIO()
    df.to_csv(towrite, index=False)
    towrite.seek(0)
    st.download_button("Download Merged CSV", towrite, file_name=filename, mime="text/csv")

# -------------------------
# Main Workflow
# -------------------------


uploaded_files = st.file_uploader("Upload CSV or Excel files", type=["csv", "xlsx", "xls"], accept_multiple_files=True)

if uploaded_files:
    file_data = {}
    for f in uploaded_files:
        df = load_file(f)
        if df is not None:
            file_data[f.name] = clean_data(df, f.name)

    file_names = list(file_data.keys())

    if len(file_names) >= 2:
        st.markdown("### Step 1: Merge Initial Two Files")
        file_1 = st.selectbox("Primary File", file_names, key="file1")
        file_2 = st.selectbox("Secondary File", [f for f in file_names if f != file_1], key="file2")

        if file_1 and file_2:
            key_1 = st.selectbox(f"Key in `{file_1}`", file_data[file_1].columns, key="key1")
            key_2 = st.selectbox(f"Key in `{file_2}`", file_data[file_2].columns, key="key2")

            if st.button("Merge Initial Files"):
                merged_df = merge_datasets(file_data[file_1], file_data[file_2], key_1, key_2)
                st.session_state['merged_df'] = merged_df
                st.session_state['used_files'] = {file_1, file_2}
                st.success(f"{file_2} merged into {file_1}. Shape: {merged_df.shape}")

    if 'merged_df' in st.session_state:
        st.markdown("### Step 2: Incremental Merging of Remaining Files")
        remaining_files = [f for f in file_names if f not in st.session_state['used_files']]

        for next_file in remaining_files:
            with st.expander(f"Merge `{next_file}`"):
                key_current = st.selectbox(f"Key from current merged data", st.session_state['merged_df'].columns, key=f"key_merged_{next_file}")
                key_new = st.selectbox(f"Key from `{next_file}`", file_data[next_file].columns, key=f"key_new_{next_file}")

                if st.button(f"Merge `{next_file}`", key=f"btn_merge_{next_file}"):
                    st.session_state['merged_df'] = merge_datasets(st.session_state['merged_df'], file_data[next_file], key_current, key_new)
                    st.session_state['used_files'].add(next_file)
                    st.success(f"{next_file} merged successfully. New shape: {st.session_state['merged_df'].shape}")

        st.markdown("---")
        st.subheader("Final Merged Data")

        st.markdown("### Step 3: Select Columns to Keep")
        all_columns = list(st.session_state['merged_df'].columns)
        selected_columns = st.multiselect("Choose columns to include in the final output", all_columns, default=all_columns)

        if selected_columns:
            final_output_df = st.session_state['merged_df'][selected_columns]
            st.dataframe(final_output_df, use_container_width=True)

            st.markdown("### Step 4: Download Output")
            user_filename = st.text_input("Enter desired output file name (no need to add .csv)", value="Merged_File")

            if user_filename:
                if not user_filename.lower().endswith('.csv'):
                    user_filename += '.csv'
                download_csv(final_output_df, filename=user_filename)
        else:
            st.warning("Please select at least one column to include in the final output.")

else:
    st.info("Upload at least 2 files to begin merging.")


# In[ ]:




