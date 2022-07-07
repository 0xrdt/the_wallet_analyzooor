import streamlit as st
import pandas as pd
import pandas as pd
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import numpy as np
import datetime

from utils.data_loaders import *
from utils.df_grid_builder import df_grid_builder

st.set_page_config(
     page_title="The Wallet Analyzoooooor",
     page_icon="🔎",
     layout="wide",
     initial_sidebar_state="expanded",
    #  menu_items={
    #      'About': 'https://0xrdt.notion.site/Interactive-Arbitrum-Explorer-41441dc5176049559bf35eb6bb1ffef8'}
 )

st.title('The Wallet Analyzoooooor')

st.markdown("Fetching the data may take some time. Please be patient. (it usually takes less than 1 minute)")

#st.sidebar.title('Choose what you want to see')
selected_sections = st.multiselect('Choose the sections you want to see:', 
										   [
												'Transactions',
												'Historical Balance',
												'Transfers'
											], 
										   default=['Historical Balance'])

start_date = st.date_input(label='Start date', value=datetime.date(2022, 6, 1)).strftime('%Y-%m-%d')
wallet_address = st.text_input(label='Wallet address', value='0x41318419cfa25396b47a94896ffa2c77c6434040')
rows_limit = st.number_input(label='Rows limit (useful if the app is crashing, but will break some charts)', 
								value=100_000, min_value=1, max_value=100_000)

st.markdown("Wallet label:")
wallet_label = load_wallet_label(wallet_address).copy()
wallet_label = wallet_label[['ADDRESS_NAME', 'LABEL_TYPE', 'LABEL_SUBTYPE', 'LABEL']]
st.write(wallet_label)

if 'Transactions' in selected_sections:

	st.markdown("## Transactions")
	
	transactions_per_wallet = load_transactions(start_date=start_date, wallet_address=wallet_address, rows_limit=rows_limit).copy()
	if len(transactions_per_wallet)==0:
		st.markdown("No transactions found.")
	else:
		transactions_per_wallet['dummy']=10
		fig = px.scatter(transactions_per_wallet, x="BLOCK_TIMESTAMP", y="LABEL", color="SIDE", size='dummy', size_max=10, opacity=0.5,
			title='Labeled Transactions over time', hover_data=['TX_FEE', 'TX_HASH', 'ETH_VALUE'])
		# fig.update_layout(xaxis_rangeslider_visible=True)
		fig.update_layout(height=500)
		fig.update_yaxes(title='Transaction per Label')
		fig.update_xaxes(title='Date')
		st.write(fig)

		should_agg_transactions = st.checkbox("Aggregate Transactions by day")
		if should_agg_transactions:
			agg_transactions_per_wallet = transactions_per_wallet\
											.set_index('BLOCK_TIMESTAMP')\
											.groupby(['SIDE', 'LABEL', pd.Grouper(freq='1d')])['TX_HASH']\
											.count()
			tmp = agg_transactions_per_wallet.reset_index()
			tmp = tmp[tmp['SIDE']=='incoming']
			fig = px.bar(tmp, x="BLOCK_TIMESTAMP", y="TX_HASH", facet_row="SIDE", color='LABEL',
					title='Aggregated Labeled Incoming Transactions over time')
			fig.update_yaxes(title='Number of Transactions')
			fig.update_xaxes(title='Date')
			fig.update_layout(height=700)
			st.write(fig)

			agg_transactions_per_wallet = transactions_per_wallet\
										.set_index('BLOCK_TIMESTAMP')\
										.groupby(['SIDE', 'LABEL', pd.Grouper(freq='1d')])['TX_HASH']\
										.count()
			tmp = agg_transactions_per_wallet.reset_index()
			tmp = tmp[tmp['SIDE']=='outgoing']
			fig = px.bar(tmp, x="BLOCK_TIMESTAMP", y="TX_HASH", facet_row="SIDE", color='LABEL',
					title='Aggregated Labeled Outgoing Transactions over time')
			fig.update_yaxes(title='Number of Transactions')
			fig.update_xaxes(title='Date')
			fig.update_layout(height=700)
			st.write(fig)

		should_show_raw_data = st.checkbox("Show transactions raw data")
		if should_show_raw_data:
			cols = [
			'TX_HASH', 'SIDE', 'BLOCK_TIMESTAMP', 'FROM_ADDRESS', 'TO_ADDRESS', 'ETH_VALUE', 'TX_FEE', 
			'ADDRESS_NAME', 'LABEL_TYPE', 'LABEL_SUBTYPE',
			'LABEL'
			]
			df = transactions_per_wallet[cols].sort_values(by='BLOCK_TIMESTAMP')
			df_grid_builder(df)

if 'Historical Balance' in selected_sections:

	st.markdown("## Historical Balance")

	erc20_balances_per_wallet = load_erc20_balances(start_date=start_date, wallet_address=wallet_address, rows_limit=rows_limit).copy()

	if len(erc20_balances_per_wallet)==0:
		st.markdown("No ERC20 balances found.")
	
	else:
		fig = px.bar(erc20_balances_per_wallet, x='BALANCE_DATE', y='AMOUNT_USD', color='SYMBOL')
		fig.update_layout(title='Daily Balances')
		fig.update_yaxes(title='Amount in USD')
		fig.update_xaxes(title='Date')
		st.write(fig)


		normalized_erc20_balances_per_wallet = erc20_balances_per_wallet.groupby(['BALANCE_DATE', 'SYMBOL'])['AMOUNT_USD'].sum()/erc20_balances_per_wallet.groupby(['BALANCE_DATE'])['AMOUNT_USD'].sum()
		normalized_erc20_balances_per_wallet = normalized_erc20_balances_per_wallet.reset_index()

		normalized_erc20_balances_per_wallet['AMOUNT_USD'] = normalized_erc20_balances_per_wallet['AMOUNT_USD']*100

		normalized_erc20_balances_per_wallet.columns=['BALANCE_DATE', 'SYMBOL', 'AMOUNT_USD_NORMALIZED']

		fig = px.bar(normalized_erc20_balances_per_wallet, x='BALANCE_DATE', y='AMOUNT_USD_NORMALIZED', color='SYMBOL')
		fig.update_layout(title='Normalized Daily Balances', height=700)
		fig.update_yaxes(title='Normalized Amount in USD')
		fig.update_xaxes(title='Date')
		st.write(fig)

		st.markdown("Most recent balance:")
		cols = ['BALANCE_DATE', 'SYMBOL', 'PRICE', 'BALANCE', 'AMOUNT_USD']
		most_recent_balance_date = erc20_balances_per_wallet['BALANCE_DATE'].max()
		most_recent_balance = erc20_balances_per_wallet[erc20_balances_per_wallet['BALANCE_DATE']==most_recent_balance_date]
		df = most_recent_balance[cols].sort_values(by=['AMOUNT_USD'], ascending=False)
		st.write(df)

if 'Transfers' in selected_sections:

	st.markdown("## Transfers")

	native_token_transfers_per_wallet = load_native_token_transfers(start_date=start_date, wallet_address=wallet_address, rows_limit=rows_limit).copy()
	token_transfers_per_wallet = load_erc20_token_transfers(start_date=start_date, wallet_address=wallet_address, rows_limit=rows_limit).copy()

	if (len(native_token_transfers_per_wallet)==0) or (len(token_transfers_per_wallet)==0):
		st.markdown("No transfers found. (right now the account needs to have both ERC20 and ETH transfers, I'll fix it later")
	else:
		agg_native_token_transfers_per_wallet = native_token_transfers_per_wallet.groupby(["LABEL", "SIDE"])[['AMOUNT_USD', 'AMOUNT']].sum()
		agg_native_token_transfers_per_wallet = agg_native_token_transfers_per_wallet.reset_index()

		fig = px.sunburst(agg_native_token_transfers_per_wallet, path=['SIDE', 'LABEL'], values='AMOUNT_USD', title='Total ETH Transfers')
		fig.update_layout(height=700)
		st.write(fig)

		agg_token_transfers_per_wallet = token_transfers_per_wallet.groupby(["LABEL", "SIDE", 'SYMBOL'])[['AMOUNT_USD', 'AMOUNT']].sum()
		agg_token_transfers_per_wallet = agg_token_transfers_per_wallet.reset_index()

		fig = px.sunburst(agg_token_transfers_per_wallet, path=['SIDE', 'SYMBOL', 'LABEL'], values='AMOUNT_USD', title='Total ERC-20 Transfers')
		fig.update_layout(height=700)
		st.write(fig)
		
		should_show_scatter_plot = st.checkbox("Show transfers scatter plot")
		if should_show_scatter_plot:

			native_token_transfers_per_wallet['dummy'] = 10
			fig = px.scatter(native_token_transfers_per_wallet, x="BLOCK_TIMESTAMP", y="LABEL", color="SIDE", 
						hover_data=['LABEL', 'AMOUNT', 'AMOUNT_USD'], size='dummy', size_max=10, opacity=0.5)
			fig.update_layout(height=500, title='Labeled ETH Transfers over time')
			fig.update_xaxes(title='Date')
			fig.update_yaxes(title='Transfer per Label')
			st.write(fig)

			token_transfers_per_wallet['dummy'] = 10
			fig = px.scatter(token_transfers_per_wallet, x="BLOCK_TIMESTAMP", y='SYMBOL',color="LABEL", facet_row="SIDE", 
					hover_data=['LABEL', 'AMOUNT', 'AMOUNT_USD'], size='dummy', size_max=10, opacity=0.5)
			fig.update_layout(height=500, title='Labeled ERC-20 Transfers over time')
			fig.update_xaxes(title='Date')
			fig.update_yaxes(title='Transfer per Label')
			st.write(fig)
		
		should_show_raw_data = st.checkbox("Show transfers raw data")
		if should_show_raw_data:
			cols = [
				'TX_HASH', 'SIDE', 'BLOCK_TIMESTAMP', 'ORIGIN_FROM_ADDRESS', 
				'ORIGIN_TO_ADDRESS', 'AMOUNT', 'AMOUNT_USD', 'ADDRESS_NAME', 
				'LABEL_TYPE', 'LABEL_SUBTYPE', 'LABEL',
			]
			df = native_token_transfers_per_wallet[cols].sort_values(by=['BLOCK_TIMESTAMP'])
			df_grid_builder(df)
		
			cols = [
				'TX_HASH', 'SIDE', 'SYMBOL', 'BLOCK_TIMESTAMP', 'ORIGIN_FROM_ADDRESS', 
				'ORIGIN_TO_ADDRESS', 'AMOUNT', 'AMOUNT_USD', 'ADDRESS_NAME', 
				'LABEL_TYPE', 'LABEL_SUBTYPE', 'LABEL',
			]
			df = token_transfers_per_wallet[cols].sort_values(by=['BLOCK_TIMESTAMP'])
			df_grid_builder(df)


