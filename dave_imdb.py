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
actor_movies = pd.read_sql_query(query, conn, params=[nconst])
actor_movies.head(actor_movies.shape[0])

# %%
# plot year on x and averagerating on y
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()
plt.figure(figsize=(12, 8))
plt.scatter(actor_movies["startyear"], actor_movies["averagerating"])
plt.xlabel("Year")
plt.ylabel("Average Rating")
plt.ylim(0, 10)
plt.title("Average Rating of Movies Over Time")
plt.show()


# %%
# show the rows of tc_costars dataframe where primaryname column is Timothy Olyphant
to = tc_costars[tc_costars["primaryname"] == "Frank Vincent"]
#to = to.merge(pd.read_sql_query("SELECT * FROM title_basics;", conn), on="tconst")
to.head(to.shape[0])



