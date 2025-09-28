import streamlit as st
import pandas as pd
import geopandas as gpd
import folium
from streamlit_folium import st_folium

cluster_means_df = pd.read_csv("/Users/sruthisakala/VTHacks2025/cluster_means.csv")

st.subheader("Cluster Means Summary")
st.dataframe(cluster_means_df.drop(columns=['num_recurring', 'num_transfers_sent', 'total_transfers_sent', 'num_transfers_received', 'total_transfers_received', 'num_deposits', 'total_deposits', 'num_withdrawals', 'total_withdrawals', 'total_p2p_count', 'total_p2p_volume','deposit_withdrawal_ratio']))

# Optional: allow filtering by cluster
#selected_cluster = st.selectbox("Select Cluster to View", cluster_means_df['cluster'].unique())
#st.dataframe(cluster_means_df[cluster_means_df['cluster'] == selected_cluster])
