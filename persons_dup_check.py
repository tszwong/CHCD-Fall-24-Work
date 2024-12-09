from py2neo import Graph
import pandas as pd
import numpy as np 
import csv
import openai
import os
from dotenv import load_dotenv

# grab the api key to use LLM
open_ai_key = os.getenv("OPENAI_API_KEY")

# Connect to the database
graph = Graph("neo4j+s://portal.chcdatabase.com:7687", auth=("developer", "3umm5rd3v"))

###
### Basic test queries to see if the connection worked
###

# TEST 1: checking a known duplicate to see the data
df = graph.run('''
    MATCH (p:Person) 
    WHERE p.id IN ['P_000739', 'P_000408']
    RETURN p
''').to_data_frame()

# TEST 2: checking a known non-duplicate to see the data
df_1 = graph.run('''
    MATCH (p:Person) 
    WHERE p.id IN ['P_000361', 'P_033053']
    RETURN p
''').to_data_frame()

# TEST 3:
df_2 = graph.run('''
    MATCH (p:Person) 
    WHERE p.id IN ['P_003936', 'P_028187']
    RETURN p
''').to_data_frame()

# TEST 4: checking relationships of a specific person with institutions
df_institutional_relationships = graph.run('''
    MATCH (p:Person {id: 'P_000408'})-[r:PRESENT_AT|FINANCIAL_PROCURATOR]->(i:Institution)
    RETURN i.name_western AS Institution, type(r) AS RelationshipType, r.start_year AS Years
''').to_data_frame()

# Display the results of the new query 
print("Institutions and Relationships for P_000408:")
print(df_institutional_relationships.to_string(), "\n")

# TEST 5: checking relationships of a specific person with corporates
df_corporate_relationships = graph.run('''
    MATCH (p:Person {id: 'P_000408'})-[r:PART_OF]->(o)
    RETURN o.name_western AS Organization, o.china_start AS Year
''').to_data_frame()

# print("Corporate Relationships for P_000408:")
# print(df_corporate_relationships.to_string(), "\n")

# for i in list(df_institutional_relationships):
#     print(df_institutional_relationships[i].tolist())
# for i in list(df_corporate_relationships):
#     print(df_corporate_relationships[i].tolist())

# display the full data frame
# pd.set_option('display.max_rows', None)
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', None)
# pd.set_option('display.max_colwidth', None)

# print(df.to_string())

persons_array = []
# Iterate over the rows of the DataFrame and extract the person data
for idx, row in df.iterrows():
    person = row.iloc[0]  # Get the person data (assuming it's stored in the first column)
    persons_array.append(person)

# Print the array to verify the results
# print(f"Persons Array:\n{persons_array}\n")

# print("Column names:", df.columns) # see what the cols are 


###
###  going through the data frames to grab the nationalities
###
nationality = {} 
not_dups = {}

for idx, row in df_1.iterrows():
    person = row.iloc[0]  # get the person
    person_id = person['id']  # get the ID of the person
    person_nationality = person.get('nationality', 'Unknown')  # get the nationality, default to 'Unknown' if not present
    
    nationality[person_id] = person_nationality # update nat dict
    
    # print(f"ID: {person_id} - Nationality: {person_nationality}")  # for debugging

# to check if nationality dict is being updated
# print("\nNationality Dictionary:", nationality)


def check_nationality_match(nationality_dict):
    """
        helper function that checks if nationalities of two ids match
        idea: if two nationalities do not match and neither are unknown, then they cannot be duplicates
    """

    nationalities = list(nationality_dict.values()) # get all nationalities from the dictionary values
    
    # check if unknown for either nationality if so we cannot assume they are not duplicates
    if 'Unknown' in nationalities:
        print("Cannot consider entries as duplicates because one or both have an unknown nationality")
    else:
        if len(set(nationalities)) == 1: # check if all values in the list are the same
            print("The nationalities match")
        else:
            print("The nationalities do not match and none unknowns --> not duplicates")
            for person_id, person_nationality in nationality_dict.items():
                print(f"ID: {person_id} - Nationality: {person_nationality}") # for debugging


# check_nationality_match(nationality)


def analyze_with_gpt(dataframe):
    """
        helper function to analyze the query results using chatgpt to check if duplicates

        GPT model used currently : gpt-3.5-turbo, gpt-4o-mini
        --- depending on budget, we can use better models like gpt-4-turbo for more accurate results

        Next step: we can consider creating a scoring system with weights to better determine accuracy
    """

    def prepare_person_data(dataframe):
        """
            helper function to prepare the information about each person in the dataframe provided
        """
        persons_array = []  # store the people from the dataframe

        for idx, row in df.iterrows():
            person = row.iloc[0]  # grab the person data

            person_data = {
                'ID': person.get('id', 'Unknown'),
                'Western Name': (person.get('given_name_western', 'Unknown') or 'Unknown') + " " + (person.get('family_name_western', 'Unknown') or 'Unknown'),
                'Nationality': person.get('nationality', 'Unknown') or 'Unknown',
                'Chinese Name (Hanzi)': (person.get('chinese_family_name_hanzi', '') or 'Unknown') + " " + (person.get('chinese_given_name_hanzi', '') or 'Unknown'),
                'Chinese Name (Romanized)': (person.get('chinese_family_name_romanized', '') or 'Unknown') + " " + (person.get('chinese_given_name_romanized', '') or 'Unknown'),
                'Gender': person.get('gender', 'Unknown') or 'Unknown',
                'Birth Year': person.get('birth_year', 'Unknown') or 'Unknown',
                'Notes': person.get('notes', 'No notes available')
            }

            persons_array.append(person_data)

        return persons_array
    

    persons = prepare_person_data(df)

    # format relationships as lists of strings for institutional and corporate relationships
    institutional_relationships = [
        f"- {row['Institution']} ({row['RelationshipType']} in {row['Years']})"
        for _, row in df_institutional_relationships.iterrows()
    ]
    corporate_relationships = [
        f"- {row['Organization']} (Year: {row['Year']})"
        for _, row in df_corporate_relationships.iterrows()
    ]

    for person_pair in zip(persons[::2], persons[1::2]):  # comparing two people at a time
        p1, p2 = person_pair

        # prompt we ask gpt to check if the queries are duplicates, **** NEED TO ADD INSTITUTIONS & RELATIONSHIP INFO
        prompt = f"""
        Please compare the following two people and determine whether they are duplicates based on their information:

        Person 1:
        - ID: {p1['ID']}
        - Western Name: {p1['Western Name']}
        - Nationality: {p1['Nationality']}
        - Chinese Name (Hanzi): {p1['Chinese Name (Hanzi)']}
        - Chinese Name (Romanized): {p1['Chinese Name (Romanized)']}
        - Gender: {p1['Gender']}
        - Birth Year: {p1['Birth Year']}
        - Notes: {p1['Notes']}\n
        - Institutional Relationships:
        {''.join(institutional_relationships) if institutional_relationships else 'None'}\n
        - Corporate Relationships:
        {''.join(corporate_relationships) if corporate_relationships else 'None'}

        Person 2:
        - ID: {p2['ID']}
        - Western Name: {p2['Western Name']}
        - Nationality: {p2['Nationality']}
        - Chinese Name (Hanzi): {p2['Chinese Name (Hanzi)']}
        - Chinese Name (Romanized): {p2['Chinese Name (Romanized)']}
        - Gender: {p2['Gender']}
        - Birth Year: {p2['Birth Year']}
        - Notes: {p2['Notes']}\n
        - Institutional Relationships:
        {''.join(institutional_relationships) if institutional_relationships else 'None'}\n
        - Corporate Relationships:
        {''.join(corporate_relationships) if corporate_relationships else 'None'}

        Are these two people likely the same individual? Please explain your reasoning and explicitly start with "Yes" or "No" in the first word of the response.
        """

        # our input to GPT
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are an expert in historical data analysis."},  # the context for the LLM to enhance results
                {"role": "user", "content": prompt}  # what we feed the LLM
            ]
        )
        
        # get and print the response from GPT
        answer = response['choices'][0]['message']['content'].strip()
        print(f"Comparison between {p1['ID']} and {p2['ID']}:")
        print(answer)
        print("\n")


#######
####### everything below is just testing to see prompt to use for gpt call
#######

def prepare_person_data(df):
    persons_array = []
    for idx, row in df.iterrows():
        person = row.iloc[0]  # get the person data
        person_data = {
            'ID': person.get('id', 'Unknown'),
            'Western Name': (person.get('given_name_western', 'Unknown') or 'Unknown') + " " + (person.get('family_name_western', 'Unknown') or 'Unknown'),
            'Nationality': person.get('nationality', 'Unknown') or 'Unknown',
            'Chinese Name (Hanzi)': (person.get('chinese_family_name_hanzi', '') or 'Unknown') + " " + (person.get('chinese_given_name_hanzi', '') or 'Unknown'),
            'Chinese Name (Romanized)': (person.get('chinese_family_name_romanized', '') or 'Unknown') + " " + (person.get('chinese_given_name_romanized', '') or 'Unknown'),
            'Gender': person.get('gender', 'Unknown') or 'Unknown',
            'Birth Year': person.get('birth_year', 'Unknown') or 'Unknown',
            'Notes': person.get('notes', 'No notes available')
        }
        persons_array.append(person_data)
    return persons_array

persons = prepare_person_data(df)

# generate the institutional and corporate relationships strings from actual data
def format_relationships(df_institutional, df_corporate):
    # format institutional relationships
    formatted_institutional_relationships = '\n    '.join(
        [f"- {row['Institution']} ({row['RelationshipType']} in {row['Years']})" 
         for _, row in df_institutional.iterrows()]
    ) if not df_institutional.empty else "None"

    # format corporate relationships
    formatted_corporate_relationships = '\n    '.join(
        [f"- {row['Organization']} (Year: {row['Year']})" 
         for _, row in df_corporate.iterrows()]
    ) if not df_corporate.empty else "None"
    
    return formatted_institutional_relationships, formatted_corporate_relationships

# get formatted relationships data
formatted_institutional_relationships, formatted_corporate_relationships = format_relationships(
    df_institutional_relationships, df_corporate_relationships
)

# create sample input for prompt used in gpt function
if len(persons) >= 2:  # look at least two people to compare
    p1, p2 = persons[0], persons[1]

    # prompt we ask gpt to check if the queries are duplicates, **** need to add institutions & relationship info
    prompt = f"""
    Please compare the following two people and determine whether they are duplicates based on their information:
    
    Person 1:
    - ID: {p1['ID']}
    - Western Name: {p1['Western Name']}
    - Nationality: {p1['Nationality']}
    - Chinese Name (Hanzi): {p1['Chinese Name (Hanzi)']}
    - Chinese Name (Romanized): {p1['Chinese Name (Romanized)']}
    - Gender: {p1['Gender']}
    - Birth Year: {p1['Birth Year']}
    - Notes: {p1['Notes']}\n
    - Institutional Relationships:
    {formatted_institutional_relationships}\n
    - Corporate Relationships:
    {formatted_corporate_relationships}

    Person 2:
    - ID: {p2['ID']}
    - Western Name: {p2['Western Name']}
    - Nationality: {p2['Nationality']}
    - Chinese Name (Hanzi): {p2['Chinese Name (Hanzi)']}
    - Chinese Name (Romanized): {p2['Chinese Name (Romanized)']}
    - Gender: {p2['Gender']}
    - Birth Year: {p2['Birth Year']}
    - Notes: {p2['Notes']}\n
    - Institutional Relationships:
    {formatted_institutional_relationships}\n
    - Corporate Relationships:
    {formatted_corporate_relationships}

    Are these two people likely the same individual? Please explain your reasoning and explicitly start with "Yes" or "No" in the first word of the response.
    """

    print(prompt)