import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import col, when_matched

st.title(":cup_with_straw: Pending Smoothie orders :cup_with_straw:")
st.write("Orders that need to be filled")

cnx = st.connection("snowflake")
session = cnx.session

# Get pending orders from Snowflake
orders_rows = session.table("smoothies.public.orders").filter(col("ORDER_FILLED") == 0).collect()

if orders_rows:
    # Convert Snowpark Rows to Pandas DataFrame for Streamlit
    orders_df = pd.DataFrame([row.as_dict() for row in orders_rows])
    editable_df = st.data_editor(orders_df)
    submitted = st.button("Submit")
    if submitted:
        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(editable_df)

        try:
            og_dataset.merge(
                edited_dataset,
                (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID']),
                [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
            )
            st.success('Order(s) updated!', icon='👍')
        except Exception as e:
            st.error(f"Something went wrong: {e}")
else:
    st.success('There are no pending orders right now.', icon='👍')
