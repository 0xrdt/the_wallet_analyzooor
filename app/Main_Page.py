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
     page_icon="🔍",
     layout="wide",
     initial_sidebar_state="expanded",
    #  menu_items={
    #      'About': 'https://0xrdt.notion.site/Interactive-Arbitrum-Explorer-41441dc5176049559bf35eb6bb1ffef8'}
 )

st.title('The Wallet Analyzoooooor')
st.markdown('check our landing page: [analyzooor.notawizard.xyz](http://analyzooor.notawizard.xyz/)')

st.info("Fetching the data may take some time. Please be patient. (it usually takes less than 1 minute)")

#st.sidebar.title('Choose what you want to see')
selected_sections = st.multiselect('Choose the sections you want to see:', 
										   [
												'Transactions',
												'Historical Balance',
												'Transfers',
												'Transactions on Other EVM Chains',
												'NFTs!'
											], 
										   default=[])

start_date = st.date_input(label='Start date', value=datetime.datetime.today()-datetime.timedelta(days=30)).strftime('%Y-%m-%d')
wallet_address = st.text_input(label='Wallet address', value='0x41318419cfa25396b47a94896ffa2c77c6434040')
rows_limit = st.number_input(label='Rows limit (useful if the app is crashing, but will break some charts)', 
								value=100_000, min_value=1, max_value=100_000)

st.markdown("""
Some wallet ideas:
- 0x41318419cfa25396b47a94896ffa2c77c6434040: celsius (good for transfers and transactions)
- 0x5DD596C901987A2b28C38A9C1DfBf86fFFc15d77: sifu (good for crosschain)
- 0x581BEf12967f06f2eBfcabb7504fA61f0326CD9A: danner.eth (good for nfts)
""")



wallet_label = st.cache(load_wallet_label, ttl=60*60*12)(wallet_address).copy()
if len(wallet_label) == 0:
	st.write("No wallet label found for the selected address")

else:
	st.markdown("Wallet label of the selected address:")
	wallet_label = wallet_label[['ADDRESS_NAME', 'LABEL_TYPE', 'LABEL_SUBTYPE', 'LABEL']]
	st.table(wallet_label.set_index("ADDRESS_NAME"))

if (('Transactions' in selected_sections) or ('Historical Balance' in selected_sections) or 
   ('Transfers' in selected_sections) or ('NFTs!' in selected_sections)):

	transactions_per_wallet, erc20_balances_per_wallet, native_token_transfers_per_wallet, \
	token_transfers_per_wallet, nft_sales_df, nft_transfers_df = st.cache(load_df_bundle, ttl=60*60*12)(wallet_address, start_date, rows_limit)
	
	transactions_per_wallet, erc20_balances_per_wallet, native_token_transfers_per_wallet, \
	token_transfers_per_wallet, nft_sales_df, nft_transfers_df = transactions_per_wallet.copy(), \
	erc20_balances_per_wallet.copy(), native_token_transfers_per_wallet.copy(), \
	token_transfers_per_wallet.copy(), nft_sales_df, nft_transfers_df.copy()

if 'Transactions' in selected_sections:

	st.markdown("## Transactions")
	
	if len(transactions_per_wallet)==0:
		st.warning("No transactions found.")
	else:

		should_show_transactions_scatter = st.checkbox("Show scatter plot of transactions")
		if should_show_transactions_scatter:

			col_txs_1, col_txs_2, col_txs_3 = st.columns(3)
			transactions_scatter_height = st.slider(label='Height of the transactions scatter plot', value=500, min_value=100, max_value=1000)

			transactions_per_wallet['dummy']=10
			fig = px.scatter(transactions_per_wallet, x="BLOCK_TIMESTAMP", y="LABEL", color="SIDE", size='dummy', size_max=10, opacity=0.5,
				title='Labeled Transactions over time', hover_data=['TX_FEE', 'TX_HASH', 'ETH_VALUE'])
			# fig.update_layout(xaxis_rangeslider_visible=True)
			fig.update_layout(height=transactions_scatter_height)
			fig.update_yaxes(title='Transaction per Label')
			fig.update_xaxes(title='Date')
			st.plotly_chart(fig, use_container_width=True)

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
			st.plotly_chart(fig, use_container_width=True)

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
			st.plotly_chart(fig, use_container_width=True)

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

	if len(erc20_balances_per_wallet)==0:
		st.warning("No ERC20 balances found.")
	
	else:
		fig = px.bar(erc20_balances_per_wallet, x='BALANCE_DATE', y='AMOUNT_USD', color='SYMBOL')
		fig.update_layout(title='Daily Balances')
		fig.update_yaxes(title='Amount in USD')
		fig.update_xaxes(title='Date')
		st.plotly_chart(fig, use_container_width=True)


		normalized_erc20_balances_per_wallet = erc20_balances_per_wallet.groupby(['BALANCE_DATE', 'SYMBOL'])['AMOUNT_USD'].sum()/erc20_balances_per_wallet.groupby(['BALANCE_DATE'])['AMOUNT_USD'].sum()
		normalized_erc20_balances_per_wallet = normalized_erc20_balances_per_wallet.reset_index()

		normalized_erc20_balances_per_wallet['AMOUNT_USD'] = normalized_erc20_balances_per_wallet['AMOUNT_USD']*100

		normalized_erc20_balances_per_wallet.columns=['BALANCE_DATE', 'SYMBOL', 'AMOUNT_USD_NORMALIZED']

		fig = px.bar(normalized_erc20_balances_per_wallet, x='BALANCE_DATE', y='AMOUNT_USD_NORMALIZED', color='SYMBOL')
		fig.update_layout(title='Normalized Daily Balances', height=700)
		fig.update_yaxes(title='Normalized Amount in USD')
		fig.update_xaxes(title='Date')
		st.plotly_chart(fig, use_container_width=True)

		should_show_most_recent_balance = st.checkbox("Show most recent balance")

		if should_show_most_recent_balance:
			st.markdown("Most recent balance:")
			cols = ['BALANCE_DATE', 'SYMBOL', 'PRICE', 'BALANCE', 'AMOUNT_USD']
			most_recent_balance_date = erc20_balances_per_wallet['BALANCE_DATE'].max()
			most_recent_balance = erc20_balances_per_wallet[erc20_balances_per_wallet['BALANCE_DATE']==most_recent_balance_date]
			df = most_recent_balance[cols].sort_values(by=['AMOUNT_USD'], ascending=False)
			st.write(df)

if 'Transfers' in selected_sections:

	st.markdown("## Transfers")

	if (len(native_token_transfers_per_wallet)==0) or (len(token_transfers_per_wallet)==0):
		st.warning("No transfers found. (right now the account needs to have both ERC20 and ETH transfers to show up here, I'll fix it later")
	else:
		agg_native_token_transfers_per_wallet = native_token_transfers_per_wallet.groupby(["LABEL", "SIDE"])[['AMOUNT_USD', 'AMOUNT']].sum()
		agg_native_token_transfers_per_wallet = agg_native_token_transfers_per_wallet.reset_index()

		should_show_sunburst = st.checkbox("Show aggregate transfers", value=True)
		if should_show_sunburst:
			fig = px.sunburst(agg_native_token_transfers_per_wallet, path=['SIDE', 'LABEL'], values='AMOUNT_USD', title='Total ETH Transfers')
			fig.update_layout(height=700)
			st.plotly_chart(fig, use_container_width=True)

			agg_token_transfers_per_wallet = token_transfers_per_wallet.groupby(["LABEL", "SIDE", 'SYMBOL'])[['AMOUNT_USD', 'AMOUNT']].sum()
			agg_token_transfers_per_wallet = agg_token_transfers_per_wallet.reset_index()

			fig = px.sunburst(agg_token_transfers_per_wallet, path=['SIDE', 'SYMBOL', 'LABEL'], values='AMOUNT_USD', title='Total ERC-20 Transfers')
			fig.update_layout(height=700)
			st.plotly_chart(fig, use_container_width=True)
		
		should_show_scatter_plot = st.checkbox("Show transfers scatter plot")
		if should_show_scatter_plot:

			col_eth_1, col_eth_2, col_eth_3 = st.columns(3)
			eth_transfers_y = col_eth_1.selectbox("Select ETH transfers Y", ['LABEL', 'AMOUNT_USD', 'AMOUNT', 'SIDE'], index=0)
			eth_transfers_color = col_eth_2.selectbox("Select ETH transfers color", ['LABEL', 'AMOUNT_USD', 'AMOUNT', 'SIDE'], index=3)
			eth_transfers_facet_row = col_eth_3.selectbox("Select ETH transfers facet row", ['LABEL', 'SIDE', None], index=2)
			eth_transfers_height = st.slider("Select ETH transfers height", min_value=200, max_value=2000, value=500, step=10)

			native_token_transfers_per_wallet['dummy'] = 10
			fig = px.scatter(native_token_transfers_per_wallet, x="BLOCK_TIMESTAMP", y=eth_transfers_y, color=eth_transfers_color, facet_row=eth_transfers_facet_row,
						hover_data={'LABEL': True, 'AMOUNT': True, 'AMOUNT_USD': True, 'dummy': False, 'SIDE': True}, 
						size='dummy', size_max=10, opacity=0.5)
			fig.update_layout(height=eth_transfers_height, title='Labeled ETH Transfers over time')
			# fig.update_xaxes(title='Date')
			# fig.update_yaxes(title='Transfer per Label')
			st.plotly_chart(fig, use_container_width=True)

			col_erc20_1, col_erc20_2, col_erc20_3 = st.columns(3)
			erc20_transfers_y = col_erc20_1.selectbox("Select ERC-20 transfers Y", ['LABEL', 'AMOUNT_USD', 'AMOUNT', 'SIDE', 'SYMBOL'], index=4)
			erc20_transfers_color = col_erc20_2.selectbox("Select ERC-20 transfers color", ['LABEL', 'AMOUNT_USD', 'AMOUNT', 'SIDE', 'SYMBOL'], index=0)
			erc20_transfers_facet_row = col_erc20_3.selectbox("Select ERC-20 transfers facet row", ['LABEL', 'SIDE', None, 'SYMBOL'], index=1)
			erc20_transfers_height = st.slider("Select ERC-20 transfers height", min_value=200, max_value=2000, value=500, step=10)

			token_transfers_per_wallet['dummy'] = 10
			fig = px.scatter(token_transfers_per_wallet, x="BLOCK_TIMESTAMP", y=erc20_transfers_y,color=erc20_transfers_color, facet_row=erc20_transfers_facet_row, 
					hover_data={'LABEL': True, 'AMOUNT': True, 'AMOUNT_USD': True, 'dummy': False, 'SIDE': True, 'SYMBOL': True}, 
					size='dummy', size_max=10, opacity=0.5)
			fig.update_layout(height=erc20_transfers_height, title='Labeled ERC-20 Transfers over time')
			# fig.update_xaxes(title='Date', row=0, col=1)
			# fig.update_yaxes(title='Transfer per Label')
			st.plotly_chart(fig, use_container_width=True)
		
		should_show_raw_data = st.checkbox("Show transfers raw data")
		if should_show_raw_data:
			cols = [
				'TX_HASH', 'SIDE', 'BLOCK_TIMESTAMP', 'ORIGIN_FROM_ADDRESS', 
				'ORIGIN_TO_ADDRESS', 'ETH_FROM_ADDRESS', 'ETH_TO_ADDRESS','AMOUNT', 'AMOUNT_USD', 'ADDRESS_NAME', 
				'LABEL_TYPE', 'LABEL_SUBTYPE', 'LABEL',
			]
			df = native_token_transfers_per_wallet[cols].sort_values(by=['BLOCK_TIMESTAMP'])
			df_grid_builder(df)
		
			cols = [
				'TX_HASH', 'SIDE', 'SYMBOL', 'BLOCK_TIMESTAMP', 'ORIGIN_FROM_ADDRESS', 
				'ORIGIN_TO_ADDRESS', 'FROM_ADDRESS', 'TO_ADDRESS','AMOUNT', 'AMOUNT_USD', 'ADDRESS_NAME', 
				'LABEL_TYPE', 'LABEL_SUBTYPE', 'LABEL',
			]
			df = token_transfers_per_wallet[cols].sort_values(by=['BLOCK_TIMESTAMP'])
			df_grid_builder(df)

if 'Transactions on Other EVM Chains' in selected_sections:
	
	st.markdown('## Transactions on Other EVM Chains')

	st.warning("Cross chain data is not available before June 2022")

	list_of_dfs = []

	other_chains = ['arbitrum', 'optimism', 'avalanche', 'bsc', 'polygon']
	for chain in other_chains:
		# print(chain)
		tmp_df = st.cache(load_transactions, ttl=60*60*12)(wallet_address.lower(), start_date, rows_limit=rows_limit, chain_name=chain).copy()
		tmp_df['chain'] = chain
		list_of_dfs.append(tmp_df)

	transactions_per_wallet_other_chains = pd.concat(list_of_dfs)

	if len(transactions_per_wallet_other_chains)==0:
		st.warning("No cross chain transfers found.")
	else:
		found_chains = list(transactions_per_wallet_other_chains['chain'].unique())
		st.write(f"Chains found: {found_chains}")

		should_show_cross_chain_scatter_plot = st.checkbox("Show cross chain scatter plot")
		if should_show_cross_chain_scatter_plot:
			transactions_per_wallet_other_chains['dummy'] = 10
			fig = px.scatter(transactions_per_wallet_other_chains, x="BLOCK_TIMESTAMP", y="LABEL", color="SIDE", size='dummy', size_max=10, opacity=0.5,
					title='Labeled Transactions over time', hover_data=['TX_FEE', 'TX_HASH', 'ETH_VALUE'], facet_row='chain')
			# fig.update_layout(xaxis_rangeslider_visible=True)
			fig.update_layout(height=500*transactions_per_wallet_other_chains['chain'].nunique())
			st.plotly_chart(fig, use_container_width=True)
		
		should_show_cross_chain_raw_data = st.checkbox("Show cross chain raw data")
		if should_show_cross_chain_raw_data:
			cols = [
			'chain', 'TX_HASH', 'SIDE', 'BLOCK_TIMESTAMP', 'FROM_ADDRESS', 'TO_ADDRESS', 'ETH_VALUE', 'TX_FEE', 
			'ADDRESS_NAME', 'LABEL_TYPE', 'LABEL_SUBTYPE',
			'LABEL'
			]
			df = transactions_per_wallet_other_chains[cols].sort_values(by='BLOCK_TIMESTAMP')
			df_grid_builder(df)

if 'NFTs!' in selected_sections:

	st.markdown("## NFTs!")

	should_show_nft_scatter_plot = st.checkbox('Show nfts scatter plots')
	if should_show_nft_scatter_plot:
		if len(nft_sales_df) > 0:
			st.subheader('NFT Secondary Market')
			nft_sales_df['dummy'] = 10

			fig = px.scatter(nft_sales_df, x='BLOCK_TIMESTAMP', y='PROJECT_NAME', color='SIDE', size='dummy', size_max=10, opacity=0.5,
						title='NFT sales/buys by project', hover_data=['PLATFORM_NAME', 'PRICE_USD', 'PRICE', 'TOKENID', 'PROJECT_NAME', 'EVENT_TYPE'])
			fig.update_layout(height=500)
			st.plotly_chart(fig, use_container_width=True)

			fig = px.scatter(nft_sales_df, x='BLOCK_TIMESTAMP', y='PRICE_USD', facet_row='SIDE', color='PROJECT_NAME',size='dummy', size_max=10, opacity=0.5,
									title='NFT sales/buys by USD Price', hover_data=['PLATFORM_NAME', 'PRICE_USD', 'PRICE', 'TOKENID', 'PROJECT_NAME', 'EVENT_TYPE'], marginal_y='box')
			fig.update_layout(height=500)
			st.plotly_chart(fig, use_container_width=True)

			nft_sales_df_agg = nft_sales_df.groupby(['PROJECT_NAME', 'SIDE']).sum()['PRICE_USD'].reset_index()
			nft_sales_df_agg.columns = ['PROJECT_NAME', 'SIDE', 'VOLUME_USD']
			fig = px.sunburst(nft_sales_df_agg, path=['SIDE', 'PROJECT_NAME'], values='VOLUME_USD', title='Aggregate NFT sales volume')
			fig.update_layout(height=700)
			st.plotly_chart(fig, use_container_width=True)

			nft_sales_df_by_platform = nft_sales_df.groupby(['PLATFORM_NAME']).agg({"PRICE_USD": "sum", "PRICE": "sum", "TX_HASH": "count"}).reset_index()
			nft_sales_df_by_platform.columns = ['PLATFORM_NAME', 'VOLUME_USD', 'VOLUME', 'COUNT']
			fig = px.bar(nft_sales_df_by_platform, x='PLATFORM_NAME', y='COUNT', title='Most used NFT platforms', hover_data=['COUNT', 'VOLUME_USD', 'VOLUME'])
			st.plotly_chart(fig, use_container_width=True)
		else:
			st.warning("No NFT sales found")
		
		if len(nft_transfers_df) > 0:
			st.subheader('NFT Mints and Transfers')

			nft_transfers_df['dummy'] = 10
			fig = px.scatter(nft_transfers_df, x='BLOCK_TIMESTAMP', y='PROJECT_NAME', color='SIDE', size='dummy', size_max=10, opacity=0.5,
						title='NFT transfers and mints by project', hover_data=['TOKENID', 'PROJECT_NAME', 'EVENT_TYPE'])
			fig.update_layout(height=500)
			st.plotly_chart(fig, use_container_width=True)

			fig = px.scatter(nft_transfers_df, x='BLOCK_TIMESTAMP', y='PROJECT_NAME', color='EVENT_TYPE', facet_row='SIDE',size='dummy', size_max=10, opacity=0.5,
						title='NFT transfers and mints by project (detailed by event type)', hover_data=['TOKENID', 'PROJECT_NAME', 'EVENT_TYPE'])
			fig.update_layout(height=500)		
			st.plotly_chart(fig, use_container_width=True)
		else:
			st.warning("No NFT transfers found")

		if len(nft_sales_df) > 0 and len(nft_transfers_df) > 0:
			st.subheader('NFT Aggregate Data')
			agg_nfts_df = pd.concat([nft_sales_df[['BLOCK_TIMESTAMP', 'SIDE', 'PROJECT_NAME']], nft_transfers_df[['BLOCK_TIMESTAMP', 'SIDE', 'PROJECT_NAME']]], ignore_index=True)
			agg_agg_nfts = agg_nfts_df.groupby(['SIDE', 'PROJECT_NAME']).count().reset_index()
			agg_agg_nfts.columns = ['SIDE', 'PROJECT_NAME', 'COUNT']
			fig = px.sunburst(agg_agg_nfts, path=['SIDE', 'PROJECT_NAME'], values='COUNT', title='Aggregate count of NFT movements (sales, mints, transfers)')
			fig.update_layout(height=700)
			st.plotly_chart(fig, use_container_width=True)

	should_show_raw_nft_data = st.checkbox('Show nfts raw data')
	if should_show_raw_nft_data:
		st.write('NFTs sales:')
		df_grid_builder(nft_sales_df)
		st.write('NFTs transfers:')
		df_grid_builder(nft_transfers_df)
