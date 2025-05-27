import pandas as pd

# Assuming clinical data is already loaded
clinical_data = pd.read_csv(r"C:\Users\PC\Desktop\gene-expression-cancer-prediction\clinical_metadata\clinical.tsv", sep="\t")

try:
    # Check if the necessary column exists
    if 'demographic.vital_status' in clinical_data.columns and 'demographic.days_to_death' in clinical_data.columns:
        
        # Handle cases for 'Alive' individuals
        clinical_data.loc[clinical_data['demographic.vital_status'] == 'Alive', 'demographic.days_to_death'] = pd.NA
        
        # For deceased individuals, fill missing 'days_to_death' with median
        deceased_data = clinical_data[clinical_data['demographic.vital_status'] == 'Deceased']
        
        if not deceased_data['demographic.days_to_death'].isna().all():
            median_deceased_days = deceased_data['demographic.days_to_death'].median()
            clinical_data.loc[clinical_data['demographic.vital_status'] == 'Deceased', 'demographic.days_to_death'] = \
                clinical_data.loc[clinical_data['demographic.vital_status'] == 'Deceased', 'demographic.days_to_death'].fillna(median_deceased_days)
        
        # Handle other columns with missing data
        clinical_data.fillna(value=pd.NA, inplace=True)

    else:
        print("Required columns 'demographic.vital_status' or 'demographic.days_to_death' are missing.")

except KeyError as e:
    print(f"KeyError: The column {e} is missing from the dataset. Please check the dataset.")

except Exception as e:
    print(f"An unexpected error occurred: {e}")

# Ensure the 'days_to_death' and 'days_to_last_follow_up' columns are numeric
clinical_data['demographic.days_to_death'] = pd.to_numeric(clinical_data['demographic.days_to_death'], errors='coerce')
clinical_data['diagnoses.days_to_last_follow_up'] = pd.to_numeric(clinical_data['diagnoses.days_to_last_follow_up'], errors='coerce')

# After handling missing values, extract relevant columns for further processing
df_extract = pd.DataFrame()
df_extract['case_id'] = clinical_data['cases.case_id']
df_extract['patient_id'] = clinical_data['cases.submitter_id']

# Convert 'age_at_diagnosis' from days to years if it's stored in days
df_extract['age_at_diagnosis'] = pd.to_numeric(clinical_data['diagnoses.age_at_diagnosis'], errors='coerce') / 365.25

# Round to 1 decimal place for consistency
df_extract['age_at_diagnosis'] = df_extract['age_at_diagnosis'].round(1)

df_extract['gender'] = clinical_data['demographic.gender']
df_extract['treatment_outcome'] = clinical_data['treatments.treatment_outcome']

# Calculate survival time in months (days_to_death or days_to_last_follow_up)
# Use 'fillna' to fill missing values in 'days_to_death' with 'days_to_last_follow_up'
clinical_data['days_survival'] = clinical_data['demographic.days_to_death'].fillna(clinical_data['diagnoses.days_to_last_follow_up'])

# Ensure 'days_survival' is numeric before performing division
clinical_data['days_survival'] = pd.to_numeric(clinical_data['days_survival'], errors='coerce')

# Calculate survival time in months, handle NaN values if present
df_extract['survival_time_months'] = (clinical_data['days_survival'] / 30.44).round(1)  # Convert from days to months

# Display the results
print(df_extract.head())

df_extract.to_csv("clinical_data_extracted.csv", index=False)
