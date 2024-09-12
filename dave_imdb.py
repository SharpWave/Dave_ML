# %%
# connect to postgresql database called imdb_pg with user postgres and password postgres
import psycopg2
import pandas as pd

# get a connection to the imdb_pg database   
def connect_to_imdb_pg():
    return psycopg2.connect("dbname=imdb_pg user=postgres password=postgres")


def search_name_basics(name, conn):   
    return pd.read_sql_query("SELECT * FROM name_basics WHERE primaryname ILIKE '%{}%';".format(name), conn)

def get_nconst(nametable, row_idx):
    return nametable.iloc[row_idx]["nconst"]   

# %%
def get_costars_and_frequencies(nconst, conn):
    # Query to get the list of costars and associated movie information
    query = """
        SELECT tp1.tconst, tp1.nconst, nb.primaryname, tb.primarytitle, tr.averagerating, tr.numvotes, tb.titletype, tb.startyear, tb.runtimeminutes, tb.genres, tb.isadult, tb.endyear
        FROM title_principals tp1
        JOIN title_principals tp2 ON tp1.tconst = tp2.tconst
        JOIN name_basics nb ON tp1.nconst = nb.nconst
        JOIN title_basics tb ON tp1.tconst = tb.tconst
        LEFT JOIN title_ratings tr ON tp1.tconst = tr.tconst
        WHERE tp2.nconst = %s 
          AND (tp2.category = 'actor' OR tp2.category = 'actress') 
          AND (tp1.category = 'actor' OR tp1.category = 'actress') 
          AND tp1.nconst != %s
    """
    
    # Get the list of movies and costars for the specified actor
    tc_costars = pd.read_sql_query(query, conn, params=[nconst, nconst])

    # Eliminate duplicated rows with the same tconst and nconst (actor playing multiple roles)
    tc_costars = tc_costars.drop_duplicates(subset=["tconst", "nconst"])

    # Keep only rows where titletype is 'movie'
    tc_costars = tc_costars[tc_costars["titletype"] == "movie"]

    # Get the number of times each nconst (costar) appears in tc_costars
    costar_counts = tc_costars["nconst"].value_counts()

    # Query to get primary names for all costars in one go
    stars_query = """
        SELECT nconst, primaryname
        FROM name_basics
        WHERE nconst IN ({})
    """.format(','.join(['%s'] * len(costar_counts.index)))

    # Fetch the costar names using a single SQL query
    star_names_df = pd.read_sql_query(stars_query, conn, params=list(costar_counts.index))

    # Add the counts to the dataframe
    star_names_df["Count"] = star_names_df["nconst"].map(costar_counts)

    # Return the dataframe sorted by count
    star_names_df = star_names_df.sort_values(by="Count", ascending=False).reset_index(drop=True)

    return star_names_df
# %%
# for the nconst of the actor, list all the movies they have acted in, and the average rating of those movies, the year the movie was released, the runtime of the movie, the genres of the movie, and the number of votes the movie has received
def get_actor_movies(nconst, conn):
    query = """
        SELECT tp.tconst, tb.primarytitle, tr.averagerating, tb.startyear, tb.runtimeminutes, tb.genres, tr.numvotes
        FROM title_principals tp
        JOIN title_basics tb ON tp.tconst = tb.tconst
        LEFT JOIN title_ratings tr ON tp.tconst = tr.tconst
        WHERE tp.nconst = %s
          AND (tp.category = 'actor' OR tp.category = 'actress')
          AND tb.titletype = 'movie'
          ORDER BY tr.averagerating DESC
    """
    return pd.read_sql_query(query, conn, params=[nconst])

def bacon_algorithm(nconst_targ, nconst_src, conn)
    path_list = [] # a list of lists of nconsts
    curr_list = []   
    checked_list = []
    depth = 0
    not_found = True
    # first, get costars of nconst_src and check if nconst_targ is in the list, curr_list
    curr_list = get_costars_and_frequencies(nconst_src, conn)
    curr_list = curr_list["nconst"].tolist()
    # make path list a list of lists corresponding to curr_list, each element is just the element of curr_list but inside a list
    path_list = [[x] for x in curr_list]
    
    if nconst_targ in curr_list:
        # add a bunch of stuff to a data structure, we'll do this later
        # idx is the index of nconst_targ in curr_list
        idx = curr_list.index(nconst_targ)
        data_out = []
        return data_out    
   
    checked_list.append(curr_list)

   # no free lunch, we have to do some work
    while (not_found):
        depth += 1
        next_list = []
        next_path_list = []
        for idx, n in enumerate(curr_list):
            temp = get_costars_and_frequencies(n, conn)
            temp = temp["nconst"].tolist()
            next_list = next_list + temp
            temp_path_list = []
            temp_path_list = [temp_path_list.append([path_list[idx], x]) for x in temp]
            next_path_list = next_path_list + temp_path_list

        curr_list = next_list
        path_list = next_path_list

        # find elements of curr_list that are in checked_list
        # remove them from curr_list
        # remove the corresponding elements from path_list
        for idx, n in enumerate(curr_list):
            if n in checked_list:
                curr_list.pop(idx)
                path_list.pop(idx)
                
        if nconst_targ in curr_list:
            not_found = False
        if depth > 6:
            not_found = False



             

    # if nconst_targ is in A, return a bunch of stuff
    # if not, add A to the "already checked" list. 
    # for everyone in A, get their costars, append to new list B
    # set A=B, increment depth counter, repeat until nconst_targ is found or depth counter exceeds 6




