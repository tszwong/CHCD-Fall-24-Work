from py2neo import Graph
import pandas as pd
import numpy as np 
import csv
import openai
import os
from dotenv import load_dotenv


graph = Graph("neo4j+s://portal.chcdatabase.com:7687", auth=("developer", "3umm5rd3v"))

def get_institution_count(graph):
    """
    Function to grab all institutions and check how many institutions there are.
    """
    
    # query to match all institutions with a name
    query = '''
        MATCH (i:Institution)
        WHERE exists(i.name_western)
        RETURN i.name_western AS Institution
    '''
    
    # convert the results to a DataFrame
    result = graph.run(query)
    df_institutions = result.to_data_frame()

    # check if the DataFrame is empty
    if df_institutions.empty:
        print("No institutions found.")
        return 0
    
    # put institutions into an array
    institutions_array = df_institutions['Institution'].tolist()
    institution_count = len(institutions_array)
    
    # debug
    # print("Institutions Array:")
    # for institution in institutions_array:
    #     print(institution)

    # debug
    #print(institutions_array)
    
    return institution_count

# Call the function and print the result
institution_count = get_institution_count(graph)
print(f"Total number of institutions: {institution_count}")