import streamlit as st
import mysql.connector
import pandas as pd
# user="phoebobtan"
# password="WalaEh@22!!!"


st.set_page_config(layout="wide")

def sqlConn(): # This is assuming na same way to connect
    print("Connecting to database...")
    try:
        connection = mysql.connector.connect(
        host="ccscloud.dlsu.edu.ph",
        port="21212",
        user="phoebobtan", # change details when we connect
        password="WalaEh@22!!!",
        database="central_node",
        use_pure = True
    )
        if connection.is_connected():
            print("Conncetion success")
        return connection
    except mysql.connector.Error as err:
        print(f"Connection error: {err}")
        return None
    

def fetch_data(query, conn):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()

    colnames = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.commit()

    df = pd.DataFrame(result, columns=colnames)
    return df

#start of front end
conn = sqlConn() 
#query of all games (Central Node?)
query = f"""
         SELECT *
         FROM `DIMGAME`;
     """
allgames = fetch_data(query, conn)
appIdNext = len(allgames) + 1
query = f"""
         SELECT *
         FROM `DIMGAME` WHERE (Windows = 1 AND Linux = 0 AND Mac = 0);
     """
games = fetch_data(query, conn)
displayGames, addGame, moveGame, deleteGame = st.tabs(["View List of Games", "Add a Game", "Update a Game", "Remove a Game"])
with displayGames:
    st.write("List of all games supported by Windows")
    st.dataframe(games)

    with st.form("SearchGame"):
        st.write("Search a Game:")
        game_names = games['GameName'].tolist() if not games.empty else []
        selected_game = st.selectbox("Select a Game", game_names)

        submitSearch = st.form_submit_button("Search")

        if submitSearch:
            
            searchQuery = f"""
            SELECT * FROM DIMGAME WHERE GameName = "{selected_game}";
            """
            searchRes = fetch_data(searchQuery, conn)
            st.dataframe(searchRes)
    
with addGame: # Add to the database
    with st.form("addGameForm"):
        st.header("Add a Game")
        st.write("Add a Game, specifying title, release date, description, and platform supported")
        
        st.write("Game Details:")
        addTitle = st.text_input("Game Title", placeholder="Enter the game's title", key="addTitle")
        release_date = st.date_input("Release Date", key="releaseDate")
        gamePrice = st.number_input("Game Price (in $)")

        st.write("Platforms Supported:")
        submitAdd = st.form_submit_button("Add Game")

        if submitAdd:
            if not addTitle:
                st.error("Please enter a game title.")
            else:
                windowsVal = 1

                # insert to db
                try:
                    query = """
                    INSERT INTO DIMGAME(appId, GameName, ReleaseDate, Price, Windows, Mac, Linux)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """
                    vals = (appIdNext, addTitle, release_date, gamePrice, windowsVal, 0, 0)
                    print("Query:", query)
                    print("Values:", vals)

                    cursor = conn.cursor()
                    conn.start_transaction(isolation_level='READ COMMITTED')
                    cursor.execute(query, vals)
                    conn.commit()
                    cursor.close()
                    st.success(f"Game '{addTitle}' added successfully.")
                except Exception as e:
                    st.error(f"An error occurred: {e}")
                    conn.rollback()

with moveGame: #remove from current table, move to a different table?
    with st.form("moveGameForm"):
        st.header("Update Game Details")
        st.write("Update Game Details or Move a Game based on supported platform availability")
        
        st.write("Game Details:")
        moveId = st.number_input("Game ID", placeholder="Enter the game's ID", key="moveId", step=1, min_value=0)
        release_date = st.date_input("Release Date", key="releaseDateMod")
        gamePrice = st.number_input("Game Price (in $)")

        st.write("Platforms Supported:")
        windowsPlat = st.checkbox("Windows")
        macPlat = st.checkbox("Mac")
        linuxPlat = st.checkbox("Linux")

        submitMove = st.form_submit_button("Update Game")
        if submitMove:
            if not moveId:
                st.error("Please enter a game ID to update.")
            if moveId not in games['AppId'].values:
                st.error(f"Game with ID {moveId} does not exist.")
            elif not (windowsPlat or macPlat or linuxPlat):
                st.error("Please select at least one platform.")
            else:
                windowsVal = 0
                macVal = 0
                linuxVal = 0
                if windowsPlat: 
                    windowsVal = 1
                if macPlat: 
                    macVal = 1
                if linuxPlat: 
                    linuxVal = 1
            
                try:
                    # build the set conditions based on which params are filled
                    set_clause = []
                    vals = []
                    if release_date:
                        set_clause.append("ReleaseDate = %s")
                        vals.append(release_date)

                    if gamePrice:
                        set_clause.append("Price = %s")
                        vals.append(gamePrice)

                    if windowsPlat is not None:
                        set_clause.append("Windows = %s")
                        vals.append(windowsVal)

                    if macPlat is not None:
                        set_clause.append("Mac = %s")
                        vals.append(macVal)

                    if linuxPlat is not None:
                        set_clause.append("Linux = %s")
                        vals.append(linuxVal)
                    query = f"""
                        UPDATE DIMGAME
                        SET {', '.join(set_clause)}
                        WHERE AppId = %s
                    """
                    vals.append(moveId)
                    print("Query:", query)
                    print("Values:", vals)

                    cursor = conn.cursor()
                    conn.start_transaction(isolation_level='READ COMMITTED')
                    cursor.execute(query, vals)
                    conn.commit()
                    cursor.close()
                    st.success(f"Game '{moveId}' updated successfully.")
                except Exception as e:
                    st.error(f"An error occurred: {e}")
                    conn.rollback()


with deleteGame: # Delete from database
    with st.form("deleteGameForm"):
        st.header("Delete Game")
        st.write("Remove a game from the list")
        deleteTitle = st.text_input("Game Title", placeholder="Enter the game's title", key="deleteTitle")
        submitDelete = st.form_submit_button("Delete Game")

        if submitDelete:
            try:
                query = f"""
                            DELETE FROM DIMGAME
                            WHERE GameName = %s
                        """
                print("Query:", query)
                print("Values:", deleteTitle)

                cursor = conn.cursor()
                conn.start_transaction(isolation_level='READ COMMITTED')
                cursor.execute(query, (deleteTitle, ))
                conn.commit()
                cursor.close()
                st.success(f"Game Deleted successfully.")
            except Exception as e:
                st.error(f"An error occurred: {e}")
                conn.rollback()

conn.close()