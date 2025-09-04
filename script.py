# %% [markdown]
# # Imports

# %%
import os
import json
import requests
import numpy as np
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv

# %% [markdown]
# # Getting data

# %%
load_dotenv(".env")

# I inserted the query params on the base_url inside Apollo's documentation page for people search
base_url = "https://api.apollo.io/api/v1/mixed_people/search?person_titles[]=marketing&person_titles[]=ceo&person_titles[]=e-commerce&person_titles[]=ecommerce&person_titles[]=growth&person_titles[]=cmo&person_titles[]=cfo&person_titles[]=expansion&person_titles[]=commercial&person_titles[]=digital&person_titles[]=marketplace&person_titles[]=cco&person_locations[]=&person_seniorities[]=owner&person_seniorities[]=founder&person_seniorities[]=c_suite&person_seniorities[]=partner&person_seniorities[]=vp&person_seniorities[]=head&person_seniorities[]=director&person_seniorities[]=manager&person_seniorities[]=senior&organization_locations[]=Brasil&organization_locations[]=Brazil&organization_num_employees_ranges[]=10%2C10000&revenue_range[min]=1000&currently_using_any_of_technology_uids[]=vtex&currently_using_any_of_technology_uids[]=shopify&currently_using_any_of_technology_uids[]=klaviyo&currently_using_any_of_technology_uids[]=hubspot&currently_using_any_of_technology_uids[]=magento&currently_using_any_of_technology_uids[]=linx"

headers = {
    "accept": "application/json",
    "Cache-Control": "no-cache",
    "Content-Type": "application/json",
    "x-api-key": os.environ["API_KEY"]
}

# %%
pagination = f"&page={1}&per_page=100"

response = requests.post(base_url + pagination, headers=headers).json()

total_pages = response["pagination"]["total_pages"]
pages = [response]

# %%
for i in tqdm(range(2, total_pages + 1), desc="Getting people data"):

    pagination = f"&page={i}&per_page=100"

    response = requests.post(base_url + pagination, headers=headers).json()

    pages.append(response)

# %%
with open("apollo_people_data.json", "w") as f:
    json.dump(pages, f, indent=2)

# %% [markdown]
# # Cleaning json file

# %%
with open("apollo_people_data.json") as f:
    data = json.load(f)

breadcrumbs = data[0]["breadcrumbs"]

# %%
people = []
for page in data:
    people = people + page["people"]

# %%
att_to_keep = ['name', 'linkedin_url', 'title', 'twitter_url', 'facebook_url', 'headline', "organization", "departments", "subdepartments", "seniority", "functions"]

sanitized_people = []
for person in tqdm(people, desc="Cleaning data: "):
    sanitized_person_dict = {}
    for att in att_to_keep:
        try:
            sanitized_person_dict[att] = person[att]
        except KeyError:
            sanitized_person_dict[att] = None
            
    sanitized_people.append(sanitized_person_dict)

# %%
with open("breadcrumbs.json", 'w') as f:
    json.dump(breadcrumbs, f, indent=2)

with open("sanitized_people_data.json", 'w') as f:
    json.dump(sanitized_people, f, indent=2)

# %% [markdown]
# # Cleaning organization data

# %%
with open("sanitized_people_data.json", 'r') as f:
    data = json.load(f)

print("Dados totais:", len(data))

non_null_data = [person for person in data if person["organization"] != None]
print("Dados sem empresas nulas:", len(non_null_data))

# %%
def flatten_json(nested_json, separator='_'):
    """
    Flattens a nested dictionary.

    Args:
        nested_json: The dictionary to flatten.
        separator: The string to use between keys.

    Returns:
        A flattened dictionary.
    """
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for key in x:
                flatten(x[key], name + key + separator)
        elif isinstance(x, list):
            is_string_list = all(isinstance(item, str) for item in x)
            if is_string_list:
                out[name[:-1]] = "; ".join(x)
            else:
                for i, item in enumerate(x):
                    flatten(item, name + str(i) + separator)
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out

# %%
cleaned_data = []
for person in non_null_data:
    normalized_person = flatten_json(person)
    cleaned_data.append(normalized_person)

print(cleaned_data[0].keys())

# %%
removed_keys = ['organization_id', "organization_primary_phone_number", 'organization_primary_phone_sanitized_number', 'organization_languages', 'organization_alexa_ranking', 'organization_publicly_traded_symbol', 'organization_publicly_traded_exchange', 'organization_primary_domain', 'organization_organization_headcount_six_month_growth', 'organization_organization_headcount_twelve_month_growth', 'organization_organization_headcount_twenty_four_month_growth']
for person in cleaned_data:
    for key in removed_keys:
        try:
            person.pop(key)
        except KeyError:
            continue

# %%
df = pd.DataFrame(cleaned_data)
df = df.replace('', np.nan)
df = df.dropna(axis=1, how="all")
df = df[['name', 'title', 'headline', 'linkedin_url', 'organization_name', 'organization_website_url', 'organization_linkedin_url', 'organization_facebook_url',
        'organization_sanitized_phone', 'organization_primary_phone_source', 'organization_founded_year',
       'organization_logo_url','organization_linkedin_uid']]
df = df.sort_values("organization_name")
df.head()

# %%
print("Empresas diferentes:", len(df["organization_name"].unique()))
df.to_csv("final_data.csv")


