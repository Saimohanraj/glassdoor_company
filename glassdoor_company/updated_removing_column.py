# import pandas as pd

# # Load the CSV file
# df = pd.read_csv('Glassdoor_company_california.csv')

# # Group by the URL and aggregate the industries into a list
# df_grouped = df.groupby('company_url')['Industry'].apply(lambda x: list(set(x))).reset_index()

# # Save the processed data back to a CSV file
# df_grouped.to_csv('processed_file.csv', index=False)

# print("Processed file saved as 'processed_file.csv'")


import pandas as pd

# Load the CSV file
df = pd.read_csv('Hawaii_sample (copy).csv')

# Group by the URL and aggregate the industries into a list, while keeping all other columns
df_grouped = df.groupby('company_url').agg({
    'id_categories': 'first',
    'company_url': 'first',
    'company_name': 'first',
    'company_size_category': 'first',
    'company_id': 'first',
    'company_headquarters': 'first',
    'location_url': 'first',
    'company_description': 'first',
    'company_review_url': 'first',
    'company_salary_url': 'first',
    'company_career_opportunities_rating': 'first',
    'company_compensation_and_benefits_rating': 'first',
    'company_culture_and_values_rating': 'first',
    'company_diversity_and_inclusion_rating': 'first',
    'company_overall_rating': 'first',
    'company_senior_management_rating': 'first',
    'company_work_life_balance_rating': 'first',
    'all_reviews_count': 'first',
    'salary_count': 'first',
    'hash': 'first',
    'industry': lambda x: list(set(x))
}).reset_index(drop=True)

# Save the processed data back to a CSV file
df_grouped.to_csv('glassdoor_company_Hawaii_sample (copy).csv', index=False)

print("Processed file saved as 'processed_file.csv'")

