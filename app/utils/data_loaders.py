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

	if data:
		transactions_per_wallet = pd.DataFrame(data['results'], columns=data['columnLabels'])
		transactions_per_wallet = transactions_per_wallet.drop_duplicates('TX_HASH')

		transactions_per_wallet['BLOCK_TIMESTAMP'] = pd.to_datetime(transactions_per_wallet['BLOCK_TIMESTAMP'])
		cols = ['ADDRESS_NAME','LABEL_TYPE','LABEL_SUBTYPE','LABEL']
		for col in cols:
			transactions_per_wallet[col] = transactions_per_wallet[col].fillna('other')

		return transactions_per_wallet

	else: 
		return pd.DataFrame(columns=['BLOCK_NUMBER', 'BLOCK_TIMESTAMP', 'BLOCK_HASH', 'TX_HASH', 'NONCE',
       'POSITION', 'ORIGIN_FUNCTION_SIGNATURE', 'FROM_ADDRESS', 'TO_ADDRESS',
       'ETH_VALUE', 'TX_FEE', 'GAS_PRICE', 'GAS_LIMIT', 'GAS_USED',
       'CUMULATIVE_GAS_USED', 'INPUT_DATA', 'STATUS', 'TX_JSON', 'BLOCKCHAIN',
       'CREATOR', 'ADDRESS', 'ADDRESS_NAME', 'LABEL_TYPE', 'LABEL_SUBTYPE',
       'LABEL', 'SIDE'])



@st.cache
def load_erc20_balances(wallet_address: str, start_date: str, rows_limit: int = 100_000):
	chain_name = 'ethereum'
	query_template = queries.erc20_balances_per_address
	query = query_template.\
		replace("$START_DATE", start_date).\
		replace("$WALLET_ADDRESS", wallet_address.lower())
	data = flipside.get_data(query + f" LIMIT {rows_limit}")

	if data:
		erc20_balances_per_address = pd.DataFrame(data['results'], columns=data['columnLabels'])
		erc20_balances_per_address = erc20_balances_per_address.drop_duplicates()

		erc20_balances_per_address = erc20_balances_per_address.dropna(subset=["AMOUNT_USD"])
		erc20_balances_per_address['BALANCE_DATE'] = pd.to_datetime(erc20_balances_per_address['BALANCE_DATE'])
		erc20_balances_per_address.sort_values(by=['BALANCE_DATE'], inplace=True)

		return erc20_balances_per_address
	else:
		return pd.DataFrame(columns=['BALANCE_DATE', 'USER_ADDRESS', 'LABEL', 'ADDRESS_NAME', 'LABEL_TYPE',
       'LABEL_SUBTYPE', 'CONTRACT_ADDRESS', 'CONTRACT_LABEL', 'SYMBOL',
       'PRICE', 'DECIMALS', 'NON_ADJUSTED_BALANCE', 'BALANCE', 'AMOUNT_USD',
       'HAS_PRICE', 'HAS_DECIMAL'])


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

	if data:
		native_token_transfers_per_wallet = pd.DataFrame(data['results'], columns=data['columnLabels'])
		native_token_transfers_per_wallet = native_token_transfers_per_wallet.drop_duplicates(subset='TX_HASH')

		native_token_transfers_per_wallet['BLOCK_TIMESTAMP'] = pd.to_datetime(native_token_transfers_per_wallet['BLOCK_TIMESTAMP'])
		cols = ['ADDRESS_NAME','LABEL_TYPE','LABEL_SUBTYPE','LABEL']
		for col in cols:
			native_token_transfers_per_wallet[col] = native_token_transfers_per_wallet[col].fillna('other')

		return native_token_transfers_per_wallet

	else:
		return pd.DataFrame(['TX_HASH', 'BLOCK_NUMBER', 'BLOCK_TIMESTAMP', 'IDENTIFIER',
       'ORIGIN_FROM_ADDRESS', 'ORIGIN_TO_ADDRESS', 'ORIGIN_FUNCTION_SIGNATURE',
       'ETH_FROM_ADDRESS', 'ETH_TO_ADDRESS', 'AMOUNT', 'AMOUNT_USD',
       'BLOCKCHAIN', 'CREATOR', 'ADDRESS', 'ADDRESS_NAME', 'LABEL_TYPE',
       'LABEL_SUBTYPE', 'LABEL', 'SIDE'])


@st.cache
def load_erc20_token_transfers(wallet_address: str, start_date: str, rows_limit: int = 100_000):
	chain_name = 'ethereum'
	query_template = queries.token_transfers_per_wallet
	query = query_template.\
		replace("$CHAIN_NAME", chain_name).\
		replace("$START_DATE", start_date).\
		replace("$WALLET_ADDRESS", wallet_address.lower())
	data = flipside.get_data(query + f" LIMIT {rows_limit}")

	if data:
		erc20_token_transfers_per_wallet = pd.DataFrame(data['results'], columns=data['columnLabels'])
		erc20_token_transfers_per_wallet = erc20_token_transfers_per_wallet.drop_duplicates(subset='TX_HASH')

		erc20_token_transfers_per_wallet['BLOCK_TIMESTAMP'] = pd.to_datetime(erc20_token_transfers_per_wallet['BLOCK_TIMESTAMP'])
		cols = ['ADDRESS_NAME','LABEL_TYPE','LABEL_SUBTYPE','LABEL']
		for col in cols:
			erc20_token_transfers_per_wallet[col] = erc20_token_transfers_per_wallet[col].fillna('other')

		return erc20_token_transfers_per_wallet

	else:
		return pd.DataFrame(['BLOCK_NUMBER', 'BLOCK_TIMESTAMP', 'TX_HASH',
       'ORIGIN_FUNCTION_SIGNATURE', 'ORIGIN_FROM_ADDRESS', 'ORIGIN_TO_ADDRESS',
       'CONTRACT_ADDRESS', 'FROM_ADDRESS', 'TO_ADDRESS', 'RAW_AMOUNT',
       'DECIMALS', 'SYMBOL', 'TOKEN_PRICE', 'AMOUNT', 'AMOUNT_USD',
       'HAS_DECIMAL', 'HAS_PRICE', '_LOG_ID', 'INGESTED_AT',
       '_INSERTED_TIMESTAMP', 'BLOCKCHAIN', 'CREATOR', 'ADDRESS',
       'ADDRESS_NAME', 'LABEL_TYPE', 'LABEL_SUBTYPE', 'LABEL', 'SIDE'])


@st.cache
def load_wallet_label(wallet_address: str, rows_limit: int = 100_000):
	chain_name = 'ethereum'
	query_template = queries.wallet_label
	query = query_template.\
		replace("$CHAIN_NAME", chain_name).\
		replace("$WALLET_ADDRESS", wallet_address.lower())
	
	data = flipside.get_data(query + f" LIMIT {rows_limit}")
	if data:

		wallet_label = pd.DataFrame(data['results'], columns=data['columnLabels'])
		wallet_label = wallet_label.drop_duplicates()
		
		return wallet_label
	else:
		return pd.DataFrame(columns=['BLOCKCHAIN', 'CREATOR', 'ADDRESS', 'ADDRESS_NAME', 'LABEL_TYPE',
       'LABEL_SUBTYPE', 'LABEL'])