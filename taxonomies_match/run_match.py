import os
import pandas as pd
from valentine import valentine_match, valentine_match_batch
from valentine.algorithms import JaccardDistanceMatcher, Cupid, Coma, SimilarityFlooding, DistributionBased
from collections import defaultdict
import pickle
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
        df.to_csv(f"./data/instances/{sheet_name}.csv", index=False)  

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

def get_df_and_names(filepath):
    all_sheets = pd.read_excel(filepath, sheet_name=None) # sheet_name=None reads all worksheets

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

def integrate_using_bridging_effect(matches:list|pd.DataFrame, threshold:float, output_dir:str, output_file:str):
    """
    ## Bridging Effect:
    - The bridging effect is similar to the idea of reusing past identified mappings.
    - For example, two semantically similar fields, e1 and e2, might not be judged as similar based on their own evidences. 
        - Nevertheless, both of them might be similar to a third field e3 in different aspects. 
        - Thus, by considering matching all three fields together, e3 can be effectively used to suggest the mapping between e1 and e2.

    Parameter:
    ----------
    `matches`: list of tupels or pd.Dataframe
        [(table1, col1, table2, col2, sim_score)];
        they are stored matches

    `threshold`: float
        range[0, 1]. The threshold for similarity score. if `sim_score` is less than the `threshold`, this pair of match will be ignore.

    `output_dir`: string
        the path for storing integrated results
    
    `output_file`: string
        the file under `output_dir` for storing integrated results
    """
    unique_group_count = 0
    if type(matches) is list:
        matches = pd.DataFrame(matches)
    
    category_uniquegroup_mapping = {}
    uniquegroup_category_mapping = defaultdict(list)
    popping_ids = []
    for row_idx, match in matches.iterrows():
        # print("match", match)
        table1, col1, table2, col2, sim_score = match['table1'], match['col1'], match['table2'], match['col2'], match['sim_score']
        if sim_score < threshold:
            continue
        name1 = f"{table1} & {col1}"
        name2 = f"{table2} & {col2}"

        """
        if `name#` is in the `category_uniquegroup_mapping` dict, then `name#` must be in a matching group.
        if both `name1` and `name2` are in a group, check if they are in the same group; 
            if it is, then skip; 
            otherwise, update the group number for one of them and their other related categories.
        """
        if name1 not in category_uniquegroup_mapping.keys() and name2 not in category_uniquegroup_mapping.keys():
            category_uniquegroup_mapping[name1] = unique_group_count
            category_uniquegroup_mapping[name2] = unique_group_count
            uniquegroup_category_mapping[unique_group_count].extend((name1, name2))
            unique_group_count += 1
        elif name1 in category_uniquegroup_mapping.keys() and name2 in category_uniquegroup_mapping.keys():
            group_id1 = category_uniquegroup_mapping[name1]
            group_id2 = category_uniquegroup_mapping[name2]
            if group_id1 == group_id2:
                continue

            for name in uniquegroup_category_mapping[group_id2]:
                category_uniquegroup_mapping[name] = group_id1
                uniquegroup_category_mapping[group_id1].append(name)
            # uniquegroup_category_mapping.pop(group_id2)
            popping_ids.append(group_id2)
        elif name1 in category_uniquegroup_mapping.keys() and name2 not in category_uniquegroup_mapping.keys():
            group_id1 = category_uniquegroup_mapping[name1]
            category_uniquegroup_mapping[name2] = group_id1
            uniquegroup_category_mapping[group_id1].append(name2)
        else:
            group_id2 = category_uniquegroup_mapping[name2]
            category_uniquegroup_mapping[name1] = group_id2
            uniquegroup_category_mapping[group_id2].append(name1)
        # print("category_uniquegroup_mapping", category_uniquegroup_mapping)
        # print("uniquegroup_category_mapping", uniquegroup_category_mapping)
        # print("popping_ids", popping_ids)
        # input()

    for id_ in popping_ids:
        uniquegroup_category_mapping.pop(id_)
    pickle.dump(category_uniquegroup_mapping, open(os.path.join(output_dir, "category_uniquegroup_mapping.pickle"), "wb"))
    pickle.dump(category_uniquegroup_mapping, open(os.path.join(output_dir, "uniquegroup_category_mapping.pickle"), "wb"))

    with open(os.path.join(output_dir, output_file), "w") as f:
        f.write("====="*4+"\n")
        for group_id, categories in uniquegroup_category_mapping.items():
            for c in categories:
                f.write(f"{c}\n")
            f.write("====="*4+"\n")

        file = "./data/nameonly/taxonomies-valentine-format.xlsx"
        dfs, tablenames = get_df_and_names(file)
        for idx, df in enumerate(dfs):
            tablename = tablenames[idx]
            for col in df.columns:
                name = f"{tablename} & {col}"
                if name not in category_uniquegroup_mapping.keys():
                    f.write(f"{name}\n")
                    f.write("====="*4+"\n")
            
if __name__ == '__main__':
    # ============================= matching ==============================================
    # excel_file_dir = "./data/instances/taxonomies-valentine-format-instances-easyconcat.xlsx"
    # export_taxonomies(excel_file_dir)

    # df1 = pd.read_csv("./data/instances/Anthonio2020-03.csv")
    # df2 = pd.read_csv("./data/instances/Du2022-07.csv")

    # matcher1 = Cupid()
    # match(df1, df2, matcher, "Anthonio2020", "Du2022")

    # dfs, names = get_df_and_names("./data/nameonly/taxonomies-valentine-format.xlsx")
    
    # matcher1 = Cupid()
    # matches1 = match_batch(dfs, dfs, matcher1, names, names)
    # store_matches(matches1, "./matches/cupid/")
    # print("Ends Cupid\n")

    # print("Starts SimilarityFlooding\n")
    # matcher2 = SimilarityFlooding()
    # matches2 = match_batch(dfs, dfs, matcher2, names, names)
    # store_matches(matches2, "./matches/similarityflooding/")
    # print("Ends SimilarityFlooding\n")

    # dfs, names = get_df_and_names("./data/instances/taxonomies-valentine-format-instances-easyconcat.xlsx")

    # print("Starts Coma instances\n")
    # matcher3 = Coma(use_instances=True)
    # matches3 = match_batch(dfs, dfs, matcher3, names, names)
    # store_matches(matches3, "./matches/coma/instance/")
    # print("Ends Coma instances\n")

    # print("Starts JaccardDistanceMatcher\n")
    # matcher4 = JaccardDistanceMatcher()
    # matches4 = match_batch(dfs, dfs, matcher4, names, names)
    # store_matches(matches4, "./matches/jaccard/")
    # print("Ends JaccardDistanceMatcher\n")

    # print("Starts DistributionBased\n")
    # matcher5 = DistributionBased()
    # matches5 = match_batch(dfs, dfs, matcher5, names, names)
    # store_matches(matches5, "./matches/distributed/")
    # print("Ends DistributionBased\n")

    # # ============================= integration ==============================================
    source_file = "./matches/coma/schema/oneone_matches.csv"
    output_dir = "./integration/coma/schema/all"
    output_file = "integration_results.txt"
    matches = pd.read_csv(source_file)
    threshold = 0.5
    integrate_using_bridging_effect(matches, threshold, output_dir, output_file)
