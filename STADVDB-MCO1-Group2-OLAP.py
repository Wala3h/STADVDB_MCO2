import streamlit as st
import mysql.connector
import pandas as pd


st.set_page_config(layout="wide")

def sqlConn():
    print("Connecting to database...")
    try:
        connection = mysql.connector.connect(
        host="localhost",
        port="3307",
        user="root",
        password="walaeh",
        database="steamgames",
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

    df = pd.DataFrame(result, columns=colnames)
    return df

def uniquePublishers(conn):
    query = """
        SELECT DevPub 
        FROM or_devpub 
    """
    publishers = fetch_data(query, conn)
    if publishers is not None and not publishers.empty:
        publishers_list = publishers['DevPub'].dropna().tolist()
        return sorted(publishers_list)
    else:
        return []


def publisherGames(publisher, conn):
    query = f"""
        SELECT dimgame.GameName
        FROM dimgame
        JOIN br_publishers ON dimgame.AppId = br_publishers.AppId
        JOIN or_devpub ON br_publishers.DevPubKey = or_devpub.DevPubKey
        WHERE or_devpub.DevPub = '{publisher}'
    """
    games = fetch_data(query, conn)
    if games is not None and not games.empty:
        game_list = games['GameName'].dropna().tolist()
        return sorted(game_list)
    else:
        return []


#start of front end
conn = sqlConn() 
game_publishers = uniquePublishers(conn)
st.sidebar.header("Steam Games Dashboard")
view_selection = st.sidebar.radio("Select Search Mode (Changing modes will clear the inputs)",
    ("General Metrics", "Search by Filter", "Game Stats"))
if view_selection == "General Metrics":
    with st.sidebar.form("General", enter_to_submit=False, border=False):
        st.subheader("General Metrics")
        CCU = st.checkbox("Top 10 Concurrent Users")
        playtime = st.checkbox("Top 10 Average Playtime")
        period = st.radio("Release Date:", ('Year only', 'Month and Year'), index=0)

        selected_year = st.number_input("Select Year", min_value=1997, max_value=2025, value=2024, step=1)
        if period == "Month and Year":
            selected_month = st.selectbox("Select Month", range(1, 13))
            selected_day = calendar.monthrange(selected_year, selected_month)[1]

        submitMetrics = st.form_submit_button("Apply")

    if submitMetrics:
        if period == "Year only":
            select_year_month = "YEAR(dimgame.ReleaseDate) AS ReleaseYear"
            where_clause = f"WHERE dimgame.ReleaseDate BETWEEN '{selected_year}-01-1' AND '{selected_year}-12-31'"
            group_by_clause = "YEAR(dimgame.ReleaseDate)"
        else:
            select_year_month = """YEAR(dimgame.ReleaseDate) AS ReleaseYear, 
                                   MONTH(dimgame.ReleaseDate) AS ReleaseMonth"""
            where_clause = f"WHERE dimgame.ReleaseDate BETWEEN '{selected_year}-{selected_month}-1' AND '{selected_year}-{selected_month}-{selected_day}'"
            group_by_clause = "YEAR(dimgame.ReleaseDate), MONTH(dimgame.ReleaseDate)"

        if CCU:
            query_ccu = f"""
                SELECT dimgame.GameName, {select_year_month},
                SUM(fact_table.PeakCCU) AS TotalPeakCCU
                FROM dimgame 
                JOIN fact_table ON dimgame.AppID = fact_table.AppID
                {where_clause}
                GROUP BY {group_by_clause}, dimgame.GameName
                ORDER BY TotalPeakCCU DESC
                LIMIT 10;
            """
            topccu = fetch_data(query_ccu, conn)
            print(query_ccu)
            print(topccu)
            st.write("Top 10 Games by Peak CCU:")
            ccu_chart = alt.Chart(topccu).mark_bar().encode(
            x=alt.X('TotalPeakCCU:Q', title='Peak CCU'),
            y=alt.Y('GameName:N', title='Game Name', sort='-x')).properties(width=800, height=400)
            st.altair_chart(ccu_chart, use_container_width=True)

        if playtime:
            query_playtime = f"""
                SELECT dimgame.GameName, {select_year_month},
                SUM(fact_table.AVGPlaytimeForever) AS AvgPlaytimeForever
                FROM dimgame 
                JOIN fact_table ON dimgame.AppID = fact_table.AppID
                {where_clause}
                GROUP BY {group_by_clause}, dimgame.GameName
                ORDER BY AvgPlaytimeForever DESC
                LIMIT 10;
            """
            topplaytime = fetch_data(query_playtime, conn)
            print(query_playtime)
            st.write("Top 10 Games by Average Playtime Forever:")
            playtime_chart = alt.Chart(topplaytime).mark_bar().encode(
            x=alt.X('AvgPlaytimeForever:Q', title='Average Playtime Forever'),
            y=alt.Y('GameName:N', title='Game Name', sort='-x')).properties(width=800, height=400)
            st.altair_chart(playtime_chart, use_container_width=True)

if view_selection == "Search by Filter":
    # Settings for filtering/display
    with st.sidebar.form("Filtering", enter_to_submit=False, border=False):
        
        st.subheader("Display:")
        GameInfoBox = st.checkbox("Game Information")
        AvgPlaytimeBox = st.checkbox("Average Playtime")

        st.subheader("Filters")
        
        categories = fetch_data("SELECT Category FROM OR_CATEGORIES", conn)
        tags = fetch_data("SELECT Tag FROM OR_TAGS", conn)
        publisher_choice = st.selectbox("Game Publisher", game_publishers, index=None)
        categories_filter = st.multiselect("Select Game Categories", categories['Category'].tolist())
        tags_filter = st.multiselect("Select Game Tags", tags['Tag'].tolist())
        MinEstOwners = number = st.number_input("Minimum Owners", value=None, min_value=0, max_value= 200000000)
        MaxEstOwners = number = st.number_input("Maximum Owners", value=None, min_value=0, max_value= 200000000)
        
        submitFilter = st.form_submit_button("Apply Filters")

    if submitFilter:
        print("Submitted")
        if GameInfoBox:
            
            joinAppend = []
            to_append = []
            from_clause = None
            if categories_filter:
                if from_clause is None:
                    from_clause = "FROM or_categories"
                    joinAppend.append("""
                    JOIN br_categories ON br_categories.CategoriesKey = or_categories.CategoriesKey
                    JOIN dimgame ON dimgame.AppId = br_categories.AppId
                    JOIN fact_table ON dimgame.AppId = fact_table.AppID                  
                    """)
                else:
                    joinAppend.append("""
                    JOIN br_categories ON dimgame.AppId = br_categories.AppId
                    JOIN or_categories ON br_categories.CategoriesKey = or_categories.CategoriesKey
                    """)
                categories_str = "', '".join(categories_filter)
                to_append.append(f"or_categories.Category IN ('{categories_str}')")

            if tags_filter:
                if from_clause is None:
                    from_clause = "FROM or_tags"
                    joinAppend.append("""
                    JOIN br_tags ON br_tags.TagKey = or_tags.TagKey
                    JOIN dimgame ON dimgame.AppId = br_tags.AppId
                    JOIN fact_table ON dimgame.AppId = fact_table.AppID
                    """)
                else:
                    joinAppend.append("""
                    JOIN br_tags ON dimgame.AppId = br_tags.AppId
                    JOIN or_tags ON br_tags.TagKey = or_tags.TagKey
                    """)
                tags_str = "', '".join(tags_filter)
                to_append.append(f"or_tags.Tag IN ('{tags_str}')")

            if publisher_choice:
                if from_clause is None:
                    from_clause = "FROM or_devpub"
                    joinAppend.append("""
                    JOIN br_publishers ON br_publishers.DevPubKey = or_devpub.DevPubKey
                    JOIN dimgame ON dimgame.AppId = br_publishers.AppId
                    JOIN fact_table ON dimgame.AppId = fact_table.AppID
                    """)
                else:
                    joinAppend.append("""
                    JOIN br_publishers ON dimgame.AppId = br_publishers.AppId
                    JOIN or_devpub ON br_publishers.DevPubKey = or_devpub.DevPubKey
                    """)
                to_append.append(f"or_devpub.DevPub = '{publisher_choice}'")
            else:
                from_clause = """FROM dimgame
                JOIN fact_table ON dimgame.AppId = fact_table.AppID
                """
            if MaxEstOwners is None and MinEstOwners is not None:
                to_append.append(f"fact_table.EstimatedOwnersMin >= {MinEstOwners}")
            if MaxEstOwners is not None and MinEstOwners is None:
                to_append.append(f"fact_table.EstimatedOwnersMax <= {MaxEstOwners}")
            elif MinEstOwners is not None and MaxEstOwners is not None:
                to_append.append(f"fact_table.EstimatedOwnersMin >= {MinEstOwners} AND fact_table.EstimatedOwnersMax <= {MaxEstOwners}")
            GameInfoquery = f"""
            SELECT dimgame.GameName, dimgame.ReleaseDate, dimgame.AboutTheGame
            {from_clause}
            """
            GameInfoquery += " ".join(joinAppend) 
            if to_append:
                GameInfoquery += " WHERE " + " AND ".join(to_append)
        if AvgPlaytimeBox:
            
            joinAppend = []
            to_append = []
            from_clause = None
            if categories_filter:
                if from_clause is None:
                    from_clause = "FROM or_categories"
                    joinAppend.append("""
                    JOIN br_categories ON br_categories.CategoriesKey = or_categories.CategoriesKey
                    JOIN dimgame ON dimgame.AppId = br_categories.AppId
                    JOIN fact_table ON dimgame.AppId = fact_table.AppID                  
                    """)
                else:
                    joinAppend.append("""
                    JOIN br_categories ON dimgame.AppId = br_categories.AppId
                    JOIN or_categories ON br_categories.CategoriesKey = or_categories.CategoriesKey
                    """)
                categories_str = "', '".join(categories_filter)
                to_append.append(f"or_categories.Category IN ('{categories_str}')")

            if tags_filter:
                if from_clause is None:
                    print("Tags first")
                    from_clause = "FROM or_tags"
                    joinAppend.append("""
                    JOIN br_tags ON br_tags.TagKey = or_tags.TagKey
                    JOIN dimgame ON dimgame.AppId = br_tags.AppId
                    JOIN fact_table ON dimgame.AppId = fact_table.AppID
                    """)
                else:
                    joinAppend.append("""
                    JOIN br_tags ON dimgame.AppId = br_tags.AppId
                    JOIN or_tags ON br_tags.TagKey = or_tags.TagKey
                    """)
                tags_str = "', '".join(tags_filter)
                to_append.append(f"or_tags.Tag IN ('{tags_str}')")

            if publisher_choice:
                if from_clause is None:
                    from_clause = "FROM or_devpub"
                    joinAppend.append("""
                    JOIN br_publishers ON br_publishers.DevPubKey = or_devpub.DevPubKey
                    JOIN dimgame ON dimgame.AppId = br_publishers.AppId
                    JOIN fact_table ON dimgame.AppId = fact_table.AppID
                    """)
                else:
                    joinAppend.append("""
                    JOIN br_publishers ON dimgame.AppId = br_publishers.AppId
                    JOIN or_devpub ON br_publishers.DevPubKey = or_devpub.DevPubKey
                    """)
                to_append.append(f"or_devpub.DevPub = '{publisher_choice}'")
            else:
                from_clause = """FROM dimgame
                JOIN fact_table ON dimgame.AppId = fact_table.AppID
                """
            if MaxEstOwners is None and MinEstOwners is not None:
                to_append.append(f"fact_table.EstimatedOwnersMin >= {MinEstOwners}")
            if MaxEstOwners is not None and MinEstOwners is None:
                to_append.append(f"fact_table.EstimatedOwnersMax <= {MaxEstOwners}")
            elif MinEstOwners is not None and MaxEstOwners is not None:
                to_append.append(f"fact_table.EstimatedOwnersMin >= {MinEstOwners} AND fact_table.EstimatedOwnersMax <= {MaxEstOwners}")
            Avgplayquery = f"""
            SELECT dimgame.GameName, fact_table.AVGPlaytimeForever
            {from_clause}
            """
            Avgplayquery += " ".join(joinAppend) 
            if to_append:
                Avgplayquery += " WHERE " + " AND ".join(to_append)

        
        if GameInfoBox:
            print(GameInfoquery)
            filtered_results = fetch_data(GameInfoquery, conn)
            filtered_results = filtered_results.sort_values(by="GameName", ascending=True)
            st.subheader("Game Information")
            st.dataframe(filtered_results, use_container_width=True, hide_index=True)
            
        if AvgPlaytimeBox:
            print(Avgplayquery)
            filtered_results = fetch_data(Avgplayquery, conn)
            filtered_results = filtered_results.sort_values(by="GameName", ascending=True)
            sortplaytime = filtered_results.sort_values(by="AVGPlaytimeForever", ascending=False)
            top10Playtime = sortplaytime.head(10)
            
            PlaytimeChart = alt.Chart(filtered_results).mark_bar(size=6).encode(
            x=alt.X('GameName:N', title='Game Name', sort=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y('AVGPlaytimeForever:Q', title='Average Playtime'),
            ).properties(
                width=1000,
                height=450   
            ).configure_axis(
                labelAngle=0
            )
            st.subheader("Average Playtime")
            st.altair_chart(PlaytimeChart, use_container_width=True)

            PlaytimeChart = PlaytimeChart.encode(
            x=alt.X('GameName:N', title='Game Name', 
                    scale=alt.Scale(padding=15))
            )

            top10chart = alt.Chart(top10Playtime).mark_bar(size=10).encode(
            y=alt.Y('GameName:N', title='Game Name', sort=None, axis=alt.Axis(labelOverlap=False)),
            x=alt.X('AVGPlaytimeForever:Q', title='Average Playtime')).properties(width=1000,height=450).configure_axis(labelAngle=0)

            st.subheader("Top 10 Games based on Average Playtime")
            st.altair_chart(top10chart, use_container_width=True)

elif view_selection == "Game Stats":
    with st.sidebar.form("Game Search", enter_to_submit=False, border=False):
        st.header("Game Search")
        all_games = fetch_data("SELECT GameName FROM dimgame", conn)
        game_names = all_games['GameName'].dropna().tolist() if not all_games.empty else []
        selected_game = st.selectbox("Select a Game", game_names)
        submitGame = st.form_submit_button("Search")

    if submitGame:
        reviewsquery = f"""
        SELECT fact_table.PositiveReviews, fact_table.NegativeReviews
        FROM dimgame JOIN fact_table ON dimgame.AppId = fact_table.AppID
        WHERE dimgame.GameName = "{selected_game}";
        """
        gameInfo = f"""
        SELECT dimgame.AboutTheGame, dimgame.ReleaseDate, fact_table.PeakCCU, fact_table.Price
        FROM dimgame JOIN fact_table ON dimgame.AppId = fact_table.AppID
        WHERE dimgame.GameName = "{selected_game}";
        """
        print(reviewsquery)  
        reviews = fetch_data(reviewsquery, conn)
        gamedetails = fetch_data(gameInfo, conn)
        if reviews is not None:

            col1, col2 = st.columns([2, 1])  #divide the pagei nto 2 colums
            labels = 'Positive Reviews', 'Negative Reviews'
            print(f"Positive: {reviews['PositiveReviews'].iloc[0]}")
            print(f"Negative: {reviews['NegativeReviews'].iloc[0]}")
            sizes = [reviews['PositiveReviews'].iloc[0],reviews['NegativeReviews'].iloc[0]]
            with col1:
                st.header(selected_game)
                price = float(gamedetails['Price'].iloc[0])
                if price == 0.00:
                    price_display = "Free"
                else:
                    price_display = f"${price:.2f}"
                st.metric(label="Release Date", value = str(gamedetails['ReleaseDate'].iloc[0]))
                st.metric(label="Peak Concurrent Users", value=float(gamedetails['PeakCCU']))
                st.metric(label="Price", value=price_display)
                
                with st.expander(label="About The Game"):
                    st.write(str(gamedetails['AboutTheGame'].iloc[0]))
            with col2:
                fig, ax = plt.subplots(figsize=(4, 3))
                ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, textprops=dict(color='white'))
                ax.axis('equal')
                ax.set_facecolor('none')
                fig.patch.set_alpha(0.0)
                st.pyplot(fig)

        else:
            print("No values foudn")

conn.close()