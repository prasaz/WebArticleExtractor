import os
import pandas as pd

script_dir = os.path.dirname(os.path.realpath(__file__))
results_path =  os.path.join(script_dir,  'input_data', 'test_exec_v1_full_columns.csv')
input_file =  os.path.join(script_dir,  'input_data', 'test_URLs.csv')

test_results_df = pd.read_csv(results_path)
input_df = pd.read_csv(input_file)

error_df = test_results_df[(test_results_df.status == "Error")]
success_df = test_results_df[(test_results_df.status == "Success")]
zerocontent_df = test_results_df[(test_results_df.content.str.len() == 0)]

print(f"Total number of URLs: {len(input_df)}")
print(f"Total number of unique URLs: {len(input_df.drop_duplicates())}")
print(f"Total number of duplicated URLs: {input_df.duplicated().sum()}")

print(f"Success count: {len(success_df)}- {round(len(success_df)/len(test_results_df)*100)}%")
print(f"Error count: {len(error_df)} - {round(len(error_df)/len(test_results_df)*100)}%")
print(f"Zero content count: {len(zerocontent_df)} - {round(len(zerocontent_df)/len(test_results_df)*100)}%")


print(f"Total count {len(success_df) + len(error_df)} - {round(len(success_df) + len(error_df))/len(test_results_df)*100}%")
print(f"Expected total count {len(test_results_df)}" )

del test_results_df
del error_df
del success_df
del zerocontent_df




