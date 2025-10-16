import streamlit as st
import pandas as pd
import pymysql
from sqlalchemy import create_engine

# -------------------------------
# Streamlit page setup
# -------------------------------
st.set_page_config(page_title="SecureCheck (MySQL)", layout="wide")
st.title("🚓 SecureCheck — Police Post Logs (MySQL + Streamlit)")

# -------------------------------
# MySQL connection configuration
# -------------------------------
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = 'aaron2013'
DB_NAME = 'project_police'
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")

# -------------------------------
# Load data safely from MySQL
# -------------------------------
@st.cache_data
def load_data():
    query = "SELECT * FROM police_post_log;"
    try:
        df = pd.read_sql(query, engine)
        st.success("✅ Successfully connected to MySQL and loaded data.")
        return df
    except Exception as e:
        st.error(f"❌ MySQL error while loading data:\n\n{e}")
        return pd.DataFrame()

df = load_data()

# -------------------------------
# Show data info
# -------------------------------
if not df.empty:
    st.markdown(f"**Connected to DB:** `{DB_NAME}` — **Rows:** {len(df):,}")
    st.dataframe(df.head())
    st.download_button("⬇️ Download Full Stops Table", data=df.to_csv(index=False), file_name="stops_full.csv")
else:
    st.warning("⚠️ No data loaded. Please check if table `stops` exists and contains records.")

# -------------------------------
# Sidebar Filters
# -------------------------------
st.sidebar.header('🔍 Quick Filters')

if not df.empty:
    country = st.sidebar.multiselect('Country', options=sorted(df['country_name'].dropna().unique()), key='country')
    violation = st.sidebar.multiselect('Violation', options=sorted(df['violation'].dropna().unique()), key='violation')
else:
    country = []
    violation = []

# Build WHERE clause
wheres = []
if country:
    quoted = ','.join([f"'{c}'" for c in country])
    wheres.append(f"country_name IN ({quoted})")
if violation:
    quoted = ','.join([f"'{v}'" for v in violation])
    wheres.append(f"violation IN ({quoted})")
where_clause = (' WHERE ' + ' AND '.join(wheres)) if wheres else ''

# -------------------------------
# Helper function to run SQL queries
# -------------------------------
def run_sql(q):
    try:
        with engine.begin() as conn:
            return pd.read_sql_query(q, conn)
    except Exception as e:
        st.error(f"SQL execution error:\n\n{e}")
        return pd.DataFrame()

# -------------------------------
# Predefined Analytical Queries
# -------------------------------
prebuilt = {
    'Top 10 vehicle_Number involved in drug-related stops':
        "SELECT vehicle_number, COUNT(*) AS cnt FROM police_post_log WHERE drugs_related_stop=1 GROUP BY vehicle_number ORDER BY cnt DESC LIMIT 10;",
    'Which vehicles were most frequently searched':
        "SELECT vehicle_number, COUNT(*) AS searches FROM police_post_log WHERE search_conducted=1 GROUP BY vehicle_number ORDER BY searches DESC LIMIT 20;",
    'Driver age group with highest arrest rate':
        """SELECT 
            CASE 
                WHEN driver_age<18 THEN '<18'
                WHEN driver_age BETWEEN 18 AND 24 THEN '18-24'
                WHEN driver_age BETWEEN 25 AND 34 THEN '25-34'
                WHEN driver_age BETWEEN 35 AND 49 THEN '35-49'
                ELSE '50+' 
            END AS age_group,
            SUM(CASE WHEN is_arrested=1 THEN 1 ELSE 0 END)*1.0/COUNT(*) AS arrest_rate,
            COUNT(*) AS police_post_log
        FROM police_post_log
        GROUP BY age_group
        ORDER BY arrest_rate DESC;""",
    'Gender distribution of drivers stopped in each country':
        "SELECT country_name, driver_gender, COUNT(*) AS cnt FROM police_post_log GROUP BY country_name, driver_gender ORDER BY country_name, cnt DESC;",
    'Race & gender combination with highest search rate':
        """SELECT driver_race, driver_gender,
                SUM(CASE WHEN search_conducted=1 THEN 1 ELSE 0 END)*1.0/COUNT(*) AS search_rate,
                COUNT(*) AS police_post_log
           FROM police_post_log
           GROUP BY driver_race, driver_gender
           ORDER BY search_rate DESC LIMIT 20;""",
    'Time of day with most traffic stops (hourly)':
        "SELECT HOUR(stop_time) AS hour, COUNT(*) AS stops FROM police_post_log WHERE stop_time IS NOT NULL GROUP BY hour ORDER BY stops DESC;",
    'Average stop duration for different violations':
        """SELECT violation,
                  AVG(CASE 
                          WHEN stop_duration LIKE '%<15' THEN 7.5
                          WHEN stop_duration LIKE '6-15' THEN 10.5
                          WHEN stop_duration LIKE '16-30' THEN 23
                          WHEN stop_duration LIKE '30+' THEN 45
                          ELSE NULL END) AS avg_minutes,
                  COUNT(*) as police_post_log
           FROM police_post_log
           GROUP BY violation
           ORDER BY avg_minutes DESC;""",
    'Are night stops more likely to lead to arrests':
        """SELECT 
                CASE 
                    WHEN HOUR(stop_time) BETWEEN 0 AND 5 THEN 'Night'
                    WHEN HOUR(stop_time) BETWEEN 6 AND 17 THEN 'Day'
                    ELSE 'Evening' 
                END AS period,
                SUM(CASE WHEN is_arrested=1 THEN 1 ELSE 0 END)/COUNT(*) AS arrest_rate,
                COUNT(*) AS police_post_log
           FROM police_post_log
           GROUP BY period
           ORDER BY arrest_rate DESC;""",
    'Violations most associated with searches or arrests':
        """SELECT violation,
                  SUM(CASE WHEN search_conducted=1 THEN 1 ELSE 0 END) AS searches,
                  SUM(CASE WHEN is_arrested=1 THEN 1 ELSE 0 END) AS arrests,
                  COUNT(*) AS police_post_log
           FROM police_post_log
           GROUP BY violation
           ORDER BY searches DESC;""",
    'Which violations most common among younger drivers (<25)':
        "SELECT violation, COUNT(*) AS cnt FROM police_post_log WHERE driver_age<25 GROUP BY violation ORDER BY cnt DESC LIMIT 15;",
    'Country with most stops where search conducted':
        """SELECT country_name,
                  SUM(CASE WHEN search_conducted=1 THEN 1 ELSE 0 END) AS searches,
                  COUNT(*) AS police_post_log
           FROM spolice_post_log
           GROUP BY country_name
           ORDER BY searches DESC LIMIT 20;"""
}

# -------------------------------
# Main Layout (Queries and Results)
# -------------------------------
col1, col2 = st.columns([2, 1])

with col2:
    selection = st.selectbox('📊 Choose an analysis', options=list(prebuilt.keys()))
    if st.button('Run selected query'):
        q = prebuilt[selection]
        if where_clause:
            q = q.rstrip(';') + ' ' + where_clause + ';'
        res = run_sql(q)
        st.write(f"**Result — {selection}**")
        st.dataframe(res)
        if not res.empty:
            csv = res.to_csv(index=False)
            st.download_button('⬇️ Download CSV', data=csv, file_name='query_results.csv')

with col1:
    st.subheader('🧠 Custom SQL Query')
    custom_sql = st.text_area('Enter your SQL (SELECT only recommended):', height=180)
    if st.button('Run custom SQL'):
        if custom_sql.strip():
            out = run_sql(custom_sql)
            st.dataframe(out)
            if not out.empty:
                st.download_button('⬇️ Download CSV', data=out.to_csv(index=False), file_name='custom_query.csv')
        else:
            st.warning("Please enter a SQL query first.")

# -------------------------------
# Record Insertion Form
# -------------------------------
with st.expander("➕ Insert New Stop Record"):
    with st.form("insert_form"):
        stop_date = st.date_input("Stop Date")
        stop_time = st.time_input("Stop Time")
        country_name = st.text_input("Country Name")
        driver_gender = st.selectbox("Driver Gender", ["Male", "Female", "Other"])
        driver_age_raw = st.number_input("Driver Age Raw", min_value=0)
        driver_age = st.number_input("Driver Age", min_value=0)
        driver_race = st.text_input("Driver Race")
        violation_raw = st.text_input("Violation Raw")
        violation = st.text_input("Violation")
        search_conducted = st.checkbox("Search Conducted")
        search_type = st.text_input("Search Type")
        stop_outcome = st.text_input("Stop Outcome")
        is_arrested = st.checkbox("Is Arrested")
        stop_duration = st.selectbox("Stop Duration", ["<15", "6-15", "16-30", "30+"])
        drugs_related_stop = st.checkbox("Drugs Related Stop")
        vehicle_number = st.text_input("Vehicle Number")

        submitted = st.form_submit_button("Insert Record")
        if submitted:
            insert_query = f"""
    INSERT INTO police_post_log (
        stop_date, stop_time, country_name, driver_gender, driver_age_raw, driver_age,
        driver_race, violation_raw, violation, search_conducted, search_type,
        stop_outcome, is_arrested, stop_duration, drugs_related_stop, vehicle_number
    ) VALUES (
        '{stop_date}', '{stop_time}', '{country_name}', '{driver_gender}', {driver_age_raw}, {driver_age},
        '{driver_race}', '{violation_raw}', '{violation}', {int(search_conducted)}, '{search_type}',
        '{stop_outcome}', {int(is_arrested)}, '{stop_duration}', {int(drugs_related_stop)}, '{vehicle_number}'
    );
"""
