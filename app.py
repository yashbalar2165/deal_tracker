import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import io
import xlsxwriter
import plotly.express as px
from gspread.exceptions import SpreadsheetNotFound, APIError
import json

today = datetime.today().date()
default_start = today - timedelta(days=30)
default_end = today

# --- Google Sheets Integration ---
SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file"]
SHEET_ID = "1cAN0KG9XqLP8UxaqBnryjLtW5r9RJxOfKMNRiZS_TTE"  # Keep your current sheet ID

@st.cache_resource
def get_sheets_client():
    try:
        # Load credentials from Streamlit secrets instead of local JSON file
        creds_dict = json.loads(st.secrets["GOOGLE_SHEETS_CREDS"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, SCOPE)
        client = gspread.authorize(creds)
        return client
    except SpreadsheetNotFound:
        st.error(f"Spreadsheet with ID '{SHEET_ID}' not found. Ensure it exists and is shared with the Service Account.")
        st.stop()
    except APIError as e:
        st.error(f"Google API error: {str(e)}")
        st.stop()
    except Exception as e:
        st.error(f"Error connecting to Google Sheets: {str(e)}")
        st.stop()


def load_sheet(sheet_name):
    client = get_sheets_client()
    try:
        spreadsheet = client.open_by_key(SHEET_ID)
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_records()
        return pd.DataFrame(data)
    except SpreadsheetNotFound:
        st.error(f"Spreadsheet with ID '{SHEET_ID}' not found or not shared with the service account.")
        st.stop()
    except APIError as e:
        st.error(f"API error accessing sheet: {str(e)}")
        st.stop()
    except Exception as e:
        st.error(f"Error loading sheet '{sheet_name}': {str(e)}")
        st.stop()

def save_to_sheet(sheet_name, df):
    client = get_sheets_client()
    try:
        spreadsheet = client.open_by_key(SHEET_ID)
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    except Exception as e:
        st.error(f"Error saving to sheet '{sheet_name}': {str(e)}")
        st.stop()

def append_to_sheet(sheet_name, row):
    client = get_sheets_client()
    try:
        spreadsheet = client.open_by_key(SHEET_ID)
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.append_row(row)
    except Exception as e:
        st.error(f"Error appending to sheet '{sheet_name}': {str(e)}")
        st.stop()

def update_deal_status(deal_id, new_status):
    deals_df = load_deals()
    index = deals_df[deals_df['Deal_ID'] == deal_id].index[0]
    client = get_sheets_client()
    try:
        spreadsheet = client.open_by_key(SHEET_ID)
        worksheet = spreadsheet.worksheet('Deals')
        worksheet.update_cell(index + 2, deals_df.columns.get_loc('Status') + 1, new_status)  # +2 for header and 0-index
    except Exception as e:
        st.error(f"Error updating status for Deal ID {deal_id}: {str(e)}")
        st.stop()

# --- Data Loaders with Caching ---
@st.cache_data(ttl=60)  # Cache for 1 minute to allow updates
def load_deals():
    deals_df = load_sheet('Deals')
    if not deals_df.empty:
        deals_df['Deal_ID'] = deals_df['Deal_ID'].astype(int)
        deals_df['Agreed_From_Party'] = deals_df['Agreed_From_Party'].astype(float)
        deals_df['Agreed_To_Contractor'] = deals_df['Agreed_To_Contractor'].astype(float)
        deals_df['Start_Date'] = pd.to_datetime(deals_df['Start_Date'], errors='coerce', format='%Y-%m-%d')
    return deals_df

@st.cache_data(ttl=60)
def load_transactions():
    trans_df = load_sheet('Transactions')
    if not trans_df.empty:
        trans_df['Deal_ID'] = trans_df['Deal_ID'].astype(int)
        trans_df['Received_From_Party'] = trans_df['Received_From_Party'].astype(float)
        trans_df['Paid_To_Contractor'] = trans_df['Paid_To_Contractor'].astype(float)
        trans_df['Date'] = pd.to_datetime(trans_df['Date'], errors='coerce', format='%Y-%m-%d')
    return trans_df

# --- Helper Functions ---
def generate_deal_id():
    deals_df = load_deals()
    return deals_df['Deal_ID'].max() + 1 if not deals_df.empty else 1

def calculate_totals(deal_id):
    trans_df = load_transactions()
    deal_trans = trans_df[trans_df['Deal_ID'] == deal_id]
    total_received = deal_trans['Received_From_Party'].sum()
    total_paid = deal_trans['Paid_To_Contractor'].sum()
    return total_received, total_paid

def check_and_update_status(deal_id):
    deals_df = load_deals()
    deal = deals_df[deals_df['Deal_ID'] == deal_id].iloc[0]
    total_received, total_paid = calculate_totals(deal_id)
    if total_received >= deal['Agreed_From_Party'] and total_paid >= deal['Agreed_To_Contractor']:
        if deal['Status'] != 'Completed':
            update_deal_status(deal_id, 'Completed')
    else:
        if deal['Status'] != 'Pending':
            update_deal_status(deal_id, 'Pending')

def get_dashboard_data():
    deals_df = load_deals()
    trans_df = load_transactions()
    if deals_df.empty:
        return pd.DataFrame()
    
    aggregates = trans_df.groupby('Deal_ID').agg(
        Total_Received=('Received_From_Party', 'sum'),
        Total_Paid=('Paid_To_Contractor', 'sum')
    ).reset_index()
    
    dashboard_df = deals_df.merge(aggregates, on='Deal_ID', how='left').fillna(0)
    dashboard_df['Remaining_From_Party'] = dashboard_df['Agreed_From_Party'] - dashboard_df['Total_Received']
    dashboard_df['Remaining_To_Contractor'] = dashboard_df['Agreed_To_Contractor'] - dashboard_df['Total_Paid']
    dashboard_df['Profit'] = dashboard_df['Agreed_From_Party'] - dashboard_df['Agreed_To_Contractor']  # Profit is based on agreed amounts
    return dashboard_df[['Deal_ID', 'Party', 'Contractor','Agreed_From_Party','Agreed_To_Contractor', 'Status', 'Total_Received', 'Total_Paid', 
                         'Remaining_From_Party', 'Remaining_To_Contractor', 'Profit', 'Start_Date']]

# --- Streamlit App ---
st.set_page_config(page_title="Business Deal Tracker", layout="wide")
st.title("Business Deal Tracker")

# Navigation
page = st.sidebar.selectbox("Navigate", ["Add New Deal", "Update Transaction", "Dashboard"])

if page == "Add New Deal":
    st.header("Add New Deal")
    with st.form("add_deal_form"):
        party = st.text_input("Party Name")
        contractor = st.text_input("Contractor Name")
        agreed_from_party = st.number_input("Agreed Amount From Party", min_value=0.0, step=0.01)
        agreed_to_contractor = st.number_input("Agreed Amount To Contractor", min_value=0.0, step=0.01)
        start_date = st.date_input("Start Date", value=None)
        submitted = st.form_submit_button("Add Deal")
        
        if submitted:
            if not party or not contractor or agreed_from_party <= 0 or agreed_to_contractor <= 0 or start_date is None:
                st.error("Please fill all fields with valid values.")
            else:
                try:
                    deal_id = int(generate_deal_id())  # Convert deal_id to Python int
                    new_deal = [
                        deal_id,  # Now a Python int
                        party,
                        contractor,
                        float(agreed_from_party),  # Ensure float for consistency
                        float(agreed_to_contractor),  # Ensure float for consistency
                        'Pending',
                        start_date.strftime('%Y-%m-%d')
                    ]
                    append_to_sheet('Deals', new_deal)
                    st.success(f"Deal {deal_id} added successfully!")
                    st.cache_data.clear()  # Clear cache after update
                except Exception as e:
                    st.error(f"Error adding deal: {str(e)}")

elif page == "Update Transaction":
    st.header("Update Transaction")
    deals_df = load_deals()
    deals_df=deals_df[deals_df['Status'] == 'Pending']
    if deals_df.empty:
        st.info("No deals available. Add a new deal first.")
    else:
        deal_options = deals_df.apply(lambda row: f"{row['Deal_ID']} - {row['Party']} / {row['Contractor']}", axis=1)
        selected_deal_str = st.selectbox("Select Deal", deal_options)
        deal_id = int(selected_deal_str.split(' - ')[0])
        
        with st.form("update_transaction_form"):
            received = st.number_input("Amount Received From Party (0 if none)", min_value=0.0, step=0.01)
            paid = st.number_input("Amount Paid To Contractor (0 if none)", min_value=0.0, step=0.01)
            trans_date = st.date_input("Transaction Date", value=None)
            submitted = st.form_submit_button("Add Transaction")
            
            if submitted:
                if received == 0 and paid == 0:
                    st.error("At least one amount should be greater than 0.")
                elif trans_date is None:
                    st.error("Please select a valid transaction date.")
                else:
                    try:
                        new_trans = [deal_id, received, paid, trans_date.strftime('%Y-%m-%d')]
                        append_to_sheet('Transactions', new_trans)
                        st.cache_data.clear()
                        check_and_update_status(deal_id)
                        st.success("Transaction added successfully!")
                        st.cache_data.clear()
                    except Exception as e:
                        st.error(f"Error adding transaction: {str(e)}")

if page == "Dashboard":
    st.header("Dashboard")
    dashboard_df = get_dashboard_data()
    if dashboard_df.empty:
        st.info("No deals available.")
    else:
        # Filters
        st.subheader("Filters")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            party_filter = st.text_input("Filter by Party")
        with col2:
            contractor_filter = st.text_input("Filter by Contractor")
        with col3:
            start_date = st.date_input("Start Date", value=None, help="Select start date or leave blank for all dates")
        with col4:
            end_date = st.date_input("End Date", value=None, help="Select end date or leave blank for all dates")
        
        # Quick date range selection
        date_range_option = st.selectbox(
            "Quick Date Range",
            ["Custom", "Last Week", "Last Month", "Last Year"],
            index=0
        )

        filtered_df = dashboard_df.copy()

        # Apply quick date range filters
        if date_range_option != "Custom":
            end_date = datetime.datetime.today().date()
            if date_range_option == "Last Week":
                start_date = end_date - timedelta(days=7)
            elif date_range_option == "Last Month":
                start_date = end_date - timedelta(days=30)
            elif date_range_option == "Last Year":
                start_date = end_date - timedelta(days=365)
        
        # Apply filters
        if party_filter:
            filtered_df = filtered_df[filtered_df['Party'].str.contains(party_filter, case=False, na=False)]
        if contractor_filter:
            filtered_df = filtered_df[filtered_df['Contractor'].str.contains(contractor_filter, case=False, na=False)]
        if start_date and end_date:
            filtered_df = filtered_df[(filtered_df['Start_Date'] >= pd.to_datetime(start_date)) & 
                                    (filtered_df['Start_Date'] <= pd.to_datetime(end_date))]
        
        # Tabs for Pending and Completed
        tab1, tab2 = st.tabs(["Pending Deals", "Completed Deals"])
        
        with tab1:
            pending_df = filtered_df[filtered_df['Status'] == 'Pending']
            st.dataframe(pending_df, use_container_width=True)
        
        with tab2:
            completed_df = filtered_df[filtered_df['Status'] == 'Completed']
            st.dataframe(completed_df, use_container_width=True)
        
        # Summary Metrics
        st.subheader("Summary")
        total_profit = dashboard_df['Profit'].sum()
        pending_receivables = filtered_df[filtered_df['Status'] == 'Pending']['Remaining_From_Party'].sum()
        pending_payables = filtered_df[filtered_df['Status'] == 'Pending']['Remaining_To_Contractor'].sum()
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Profit", f"{total_profit:.2f}")
        col2.metric("Pending Receivables", f"{pending_receivables:.2f}")
        col3.metric("Pending Payables", f"{pending_payables:.2f}")
        
        # Export to Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            filtered_df.to_excel(writer, sheet_name='Deals', index=False)
        excel_data = output.getvalue()
        st.download_button(
            label="Export Filtered Data to Excel",
            data=excel_data,
            file_name="deals_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
        # Pro# Profit Chart (Monthly) using Plotly
        if not filtered_df.empty:
            filtered_df['Month'] = filtered_df['Start_Date'].dt.to_period('M').dt.to_timestamp()
            monthly_profit = (
                filtered_df.groupby('Month')['Profit']
                .sum()
                .reset_index()
                .sort_values('Month')
            )
            st.subheader("Monthly Profit Chart")
            fig = px.bar(
                monthly_profit,
                x='Month',
                y='Profit',
                text='Profit',
                title='Monthly Profit by Month'
            )
            fig.update_traces(texttemplate='%{text:.2f}', textposition='outside')
            fig.update_layout(xaxis_title="Month", yaxis_title="Profit", bargap=0.3)
            st.plotly_chart(fig, use_container_width=True)

