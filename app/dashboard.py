# app/dashboard.py (ฉบับแก้ไข)

import streamlit as st
import pandas as pd
from cassandra.cluster import Cluster
import time

# ฟังก์ชันสำหรับเชื่อมต่อและดึงข้อมูล (พร้อม Retry Logic)
def get_cassandra_data(query):
    for i in range(3):  # พยายามเชื่อมต่อ 3 ครั้ง
        try:
            cluster = Cluster(['cassandra'], port=9042)
            session = cluster.connect('movie_data')
            print("Successfully connected to Cassandra.")
            rows = session.execute(query)
            return pd.DataFrame(rows)
        except Exception as e:
            print(f"Connection attempt {i+1} failed.")
            if i < 2:  # ถ้ายังไม่ใช่ครั้งสุดท้าย
                print("Retrying in 5 seconds...")
                time.sleep(5)
            else: # ถ้าเป็นครั้งสุดท้ายแล้ว ให้โยน error ออกไป
                raise e

# --- สร้างหน้าเว็บ ---
st.set_page_config(layout="wide")
st.title("🎬 The Movies Dataset Dashboard")

st.header("Top 100 Movies")
try:
    movies_df = get_cassandra_data("SELECT * FROM movies LIMIT 100;")
    st.dataframe(movies_df)

    st.header("Latest 100 Ratings")
    ratings_df = get_cassandra_data("SELECT * FROM ratings LIMIT 100;")
    st.dataframe(ratings_df)

except Exception as e:
    st.error(f"Could not connect to Cassandra: {e}")