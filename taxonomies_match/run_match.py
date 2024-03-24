import os
import pandas as pd
from valentine import valentine_match, valentine_match_batch
from valentine.algorithms import JaccardDistanceMatcher, Cupid, Coma, SimilarityFlooding
import pprint
pp = pprint.PrettyPrinter(indent=4, sort_dicts=False)

def export_taxonomies(filepath):
    all_sheets = pd.read_excel(filepath, sheet_name=None) # sheet_name=None reads all worksheets

    for sheet_name, df in all_sheets.items():
        print(f"Processing sheet: {sheet_name}") 
        # headers = df.iloc[:1]
        # print(headers)
        # print(df.columns)
        # exit()
        df.to_csv(f"./data/{sheet_name}.csv", index=False)  

def match_batch(dfs1, dfs2, matcher, dfs1_names, dfs2_names):
    matches = valentine_match_batch(dfs1, dfs2, matcher, dfs1_names, dfs2_names)

    # MatcherResults is a wrapper object that has several useful
    # utility/transformation functions
    print("Found the following matches:")
    pp.pprint(matches)

    print("\nGetting the one-to-one matches:")
    pp.pprint(matches.one_to_one())

    return matches

def match(df1, df2, matcher, df1_name, df2_name):
    matches = valentine_match(df1, df2, matcher, df1_name, df2_name)

    # MatcherResults is a wrapper object that has several useful
    # utility/transformation functions
    print("Found the following matches:")
    pp.pprint(matches)

    print("\nGetting the one-to-one matches:")
    pp.pprint(matches.one_to_one())

def df_iter():
    all_sheets = pd.read_excel("./data/taxonomies-valentine-format.xlsx", sheet_name=None) # sheet_name=None reads all worksheets

    for sheet_name, df in all_sheets.items():
        print(f"Processing sheet: {sheet_name}") 
        yield df

def df_iter_names():
    all_sheets = pd.read_excel("./data/taxonomies-valentine-format.xlsx", sheet_name=None) # sheet_name=None reads all worksheets

    for sheet_name, df in all_sheets.items():
        print(f"Processing sheet: {sheet_name}") 
        yield sheet_name

def get_df_and_names():
    all_sheets = pd.read_excel("./data/taxonomies-valentine-format.xlsx", sheet_name=None) # sheet_name=None reads all worksheets

    dfs = []
    names = []
    for sheet_name, df in all_sheets.items():
        # print(f"Processing sheet: {sheet_name}") 
        dfs.append(df)
        names.append(sheet_name)

        # if len(names) == 4:
        #     break
    return dfs, names

def store_matches(matches, store_path):
    data = []
    for (pair, sim_score) in matches.items():
        (tab1, col1) = pair[0]
        (tab2, col2) = pair[1]

        if tab1 == tab2:
            continue

        temp = [tab1, col1, tab2, col2, sim_score]
        data.append(temp)
        
    df = pd.DataFrame(data, columns=['table1', 'col1', 'table2', 'col2', 'sim_score'])
    df.to_csv(f'{store_path}/all_matches.csv')

    data_oneone = []
    for (pair, sim_score) in matches.one_to_one().items():
        (tab1, col1) = pair[0]
        (tab2, col2) = pair[1]

        if tab1 == tab2:
            continue

        temp = [tab1, col1, tab2, col2, sim_score]
        data_oneone.append(temp)

    df2 = pd.DataFrame(data_oneone, columns=['table1', 'col1', 'table2', 'col2', 'sim_score'])
    df2.to_csv(f'{store_path}/oneone_matches.csv')


if __name__ == '__main__':
    # excel_file_dir = "./data/taxonomies-valentine-format.xlsx"
    # export_taxonomies(excel_file_dir)

    df1 = pd.read_csv("./data/Zhang2015-06.csv")
    df2 = pd.read_csv("./data/Zhang2016-11.csv")

    matcher = SimilarityFlooding()
    # match(df1, df2, matcher, "Zhang2015", "Zhang2016")

    dfs, names = get_df_and_names()
    mid = len(dfs) // 2
    print(names[:mid])
    print(names[mid:])
    matches = match_batch(dfs[:mid], dfs[mid:], matcher, names[:mid], names[mid:])
    store_matches(matches, "./matches/similarityflooding/")
