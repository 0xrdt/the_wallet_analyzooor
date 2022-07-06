import pandas as pd
import streamlit as st
import utils.flipside as flipside
import utils.queries as queries

@st.cache
def load_transactions(wallet_address: str, start_date: str, rows_limit: int = 100_000):
	chain_name = 'ethereum'
	query_template = queries.transactions_per_wallet
	query = query_template.\
		replace("$CHAIN_NAME", chain_name).\
		replace("$START_DATE", start_date).\
		replace("$WALLET_ADDRESS", wallet_address.lower())
	data = flipside.get_data(query + f" LIMIT {rows_limit}")
	transactions_per_wallet = pd.DataFrame(data['results'], columns=data['columnLabels'])

	transactions_per_wallet['BLOCK_TIMESTAMP'] = pd.to_datetime(transactions_per_wallet['BLOCK_TIMESTAMP'])
	cols = ['ADDRESS_NAME','LABEL_TYPE','LABEL_SUBTYPE','LABEL']
	for col in cols:
		transactions_per_wallet[col] = transactions_per_wallet[col].fillna('other')

	return transactions_per_wallet


@st.cache
def load_erc20_balances(wallet_address: str, start_date: str, rows_limit: int = 100_000):
	chain_name = 'ethereum'
	query_template = queries.erc20_balances_per_address
	query = query_template.\
		replace("$START_DATE", start_date).\
		replace("$WALLET_ADDRESS", wallet_address.lower())
	data = flipside.get_data(query + f" LIMIT {rows_limit}")
	erc20_balances_per_address = pd.DataFrame(data['results'], columns=data['columnLabels'])

	erc20_balances_per_address = erc20_balances_per_address.dropna(subset=["AMOUNT_USD"])
	erc20_balances_per_address['BALANCE_DATE'] = pd.to_datetime(erc20_balances_per_address['BALANCE_DATE'])
	erc20_balances_per_address.sort_values(by=['BALANCE_DATE'], inplace=True)

	return erc20_balances_per_address


@st.cache
def load_native_token_transfers(wallet_address: str, start_date: str, rows_limit: int = 100_000):
	chain_name = 'ethereum'
	query_template = queries.native_token_transfers_per_wallet
	query = query_template.\
		replace("$CHAIN_NAME", chain_name).\
		replace("$START_DATE", start_date).\
		replace("$WALLET_ADDRESS", wallet_address.lower()).\
		replace("$TOKEN_NAME", "ETH")
	data = flipside.get_data(query + f" LIMIT {rows_limit}")
	native_token_transfers_per_wallet = pd.DataFrame(data['results'], columns=data['columnLabels'])

	native_token_transfers_per_wallet['BLOCK_TIMESTAMP'] = pd.to_datetime(native_token_transfers_per_wallet['BLOCK_TIMESTAMP'])
	cols = ['ADDRESS_NAME','LABEL_TYPE','LABEL_SUBTYPE','LABEL']
	for col in cols:
		native_token_transfers_per_wallet[col] = native_token_transfers_per_wallet[col].fillna('other')

	return native_token_transfers_per_wallet


@st.cache
def load_erc20_token_transfers(wallet_address: str, start_date: str, rows_limit: int = 100_000):
	chain_name = 'ethereum'
	query_template = queries.token_transfers_per_wallet
	query = query_template.\
		replace("$CHAIN_NAME", chain_name).\
		replace("$START_DATE", start_date).\
		replace("$WALLET_ADDRESS", wallet_address.lower())
	data = flipside.get_data(query + f" LIMIT {rows_limit}")
	erc20_token_transfers_per_wallet = pd.DataFrame(data['results'], columns=data['columnLabels'])

	erc20_token_transfers_per_wallet['BLOCK_TIMESTAMP'] = pd.to_datetime(erc20_token_transfers_per_wallet['BLOCK_TIMESTAMP'])
	cols = ['ADDRESS_NAME','LABEL_TYPE','LABEL_SUBTYPE','LABEL']
	for col in cols:
		erc20_token_transfers_per_wallet[col] = erc20_token_transfers_per_wallet[col].fillna('other')

	return erc20_token_transfers_per_wallet


@st.cache
def load_wallet_label(wallet_address: str, rows_limit: int = 100_000):
	chain_name = 'ethereum'
	query_template = queries.wallet_label
	query = query_template.\
		replace("$CHAIN_NAME", chain_name).\
		replace("$WALLET_ADDRESS", wallet_address.lower())
	data = flipside.get_data(query + f" LIMIT {rows_limit}")

	wallet_label = pd.DataFrame(data['results'], columns=data['columnLabels'])
	return wallet_label