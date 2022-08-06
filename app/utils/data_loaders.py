from pyexpat import native_encoding
import pandas as pd
import utils.queries as queries
import shroomdk
import os
import concurrent.futures

# get env variable API_KEY
API_KEY = os.environ.get('FLIPSIDE_API_KEY', None)

def load_transactions(wallet_address: str, start_date: str, rows_limit: int = 100_000, chain_name='ethereum'):
	query_template = queries.transactions_per_wallet
	query = query_template.\
		replace("$CHAIN_NAME", chain_name).\
		replace("$START_DATE", start_date).\
		replace("$WALLET_ADDRESS", wallet_address.lower())

	sdk = shroomdk.ShroomDK(API_KEY)
	query_result_set = sdk.query(query + f" LIMIT {rows_limit}")

	if query_result_set.rows:
		transactions_per_wallet = pd.DataFrame(query_result_set.rows, columns=query_result_set.columns)
		transactions_per_wallet = transactions_per_wallet.drop_duplicates('TX_HASH')
		transactions_per_wallet.rename(columns={'PROJECT_NAME': 'LABEL'}, inplace=True)
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



def load_erc20_balances(wallet_address: str, start_date: str, rows_limit: int = 100_000):
	chain_name = 'ethereum'
	query_template = queries.erc20_balances_per_address
	query = query_template.\
		replace("$START_DATE", start_date).\
		replace("$WALLET_ADDRESS", wallet_address.lower())
	
	sdk = shroomdk.ShroomDK(API_KEY)
	query_result_set = sdk.query(query + f" LIMIT {rows_limit}")

	if query_result_set:
		erc20_balances_per_address = pd.DataFrame(query_result_set.rows, columns=query_result_set.columns)
		erc20_balances_per_address = erc20_balances_per_address.drop_duplicates()
		erc20_balances_per_address.rename(columns={'PROJECT_NAME': 'LABEL'}, inplace=True)
		erc20_balances_per_address = erc20_balances_per_address.dropna(subset=["AMOUNT_USD"])
		erc20_balances_per_address['BALANCE_DATE'] = pd.to_datetime(erc20_balances_per_address['BALANCE_DATE'])
		erc20_balances_per_address.sort_values(by=['BALANCE_DATE'], inplace=True)

		return erc20_balances_per_address
	else:
		return pd.DataFrame(columns=['BALANCE_DATE', 'USER_ADDRESS', 'LABEL', 'ADDRESS_NAME', 'LABEL_TYPE',
       'LABEL_SUBTYPE', 'CONTRACT_ADDRESS', 'CONTRACT_LABEL', 'SYMBOL',
       'PRICE', 'DECIMALS', 'NON_ADJUSTED_BALANCE', 'BALANCE', 'AMOUNT_USD',
       'HAS_PRICE', 'HAS_DECIMAL'])



def load_native_token_transfers(wallet_address: str, start_date: str, rows_limit: int = 100_000):
	chain_name = 'ethereum'
	query_template = queries.native_token_transfers_per_wallet
	query = query_template.\
		replace("$CHAIN_NAME", chain_name).\
		replace("$START_DATE", start_date).\
		replace("$WALLET_ADDRESS", wallet_address.lower()).\
		replace("$TOKEN_NAME", "ETH")
	
	sdk = shroomdk.ShroomDK(API_KEY)
	query_result_set = sdk.query(query + f" LIMIT {rows_limit}")

	if query_result_set:
		native_token_transfers_per_wallet = pd.DataFrame(query_result_set.rows, columns=query_result_set.columns)
		native_token_transfers_per_wallet = native_token_transfers_per_wallet.drop_duplicates(subset='TX_HASH')
		native_token_transfers_per_wallet.rename(columns={'PROJECT_NAME': 'LABEL'}, inplace=True)
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



def load_erc20_token_transfers(wallet_address: str, start_date: str, rows_limit: int = 100_000):
	chain_name = 'ethereum'
	query_template = queries.token_transfers_per_wallet
	query = query_template.\
		replace("$CHAIN_NAME", chain_name).\
		replace("$START_DATE", start_date).\
		replace("$WALLET_ADDRESS", wallet_address.lower())
	
	sdk = shroomdk.ShroomDK(API_KEY)
	query_result_set = sdk.query(query + f" LIMIT {rows_limit}")

	if query_result_set:
		erc20_token_transfers_per_wallet = pd.DataFrame(query_result_set.rows, columns=query_result_set.columns)
		erc20_token_transfers_per_wallet = erc20_token_transfers_per_wallet.drop_duplicates(subset='TX_HASH')
		erc20_token_transfers_per_wallet.rename(columns={'PROJECT_NAME': 'LABEL'}, inplace=True)
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



def load_wallet_label(wallet_address: str, rows_limit: int = 100_000):
	chain_name = 'ethereum'
	query_template = queries.wallet_label
	query = query_template.\
		replace("$CHAIN_NAME", chain_name).\
		replace("$WALLET_ADDRESS", wallet_address.lower())
	
	sdk = shroomdk.ShroomDK(API_KEY)
	query_result_set = sdk.query(query + f" LIMIT {rows_limit}")

	if query_result_set:

		wallet_label = pd.DataFrame(query_result_set.rows, columns=query_result_set.columns)
		wallet_label = wallet_label.drop_duplicates()
		wallet_label.rename(columns={'PROJECT_NAME': 'LABEL'}, inplace=True)
		return wallet_label
	else:
		return pd.DataFrame(columns=['BLOCKCHAIN', 'CREATOR', 'ADDRESS', 'ADDRESS_NAME', 'LABEL_TYPE',
       'LABEL_SUBTYPE', 'LABEL'])



def load_nft_sales(wallet_address: str, start_date: str, rows_limit: int = 100_000):
	sales_template = queries.nft_sales_template
	sales_query = sales_template.\
		replace("$START_DATE", start_date).\
		replace("$WALLET_ADDRESS", wallet_address.lower()) 

	sdk = shroomdk.ShroomDK(API_KEY)
	query_result_set = sdk.query(sales_query + f" LIMIT {rows_limit}")

	if query_result_set:

		sales_df = pd.DataFrame(query_result_set.rows, columns=query_result_set.columns)
		sales_df['BLOCK_TIMESTAMP'] = pd.to_datetime(sales_df['BLOCK_TIMESTAMP'])
		sales_df['PROJECT_NAME'] = sales_df['PROJECT_NAME'].fillna('other')
		return sales_df

	else:
		return pd.DataFrame(columns=['BLOCK_TIMESTAMP', 'TX_HASH', 'EVENT_TYPE', 'NFT_ADDRESS',
					'PROJECT_NAME', 'SELLER_ADDRESS', 'BUYER_ADDRESS', 'TOKENID',
					'PLATFORM_NAME', 'PRICE_USD', 'PRICE', 'SIDE'])



def load_nft_transfers(wallet_address: str, start_date: str, rows_limit: int = 100_000):
	transfers_template = queries.nft_transfers_template
	transfers_query = transfers_template.\
		replace("$START_DATE", start_date).\
		replace("$WALLET_ADDRESS", wallet_address.lower()) 
	
	sdk = shroomdk.ShroomDK(API_KEY)
	query_result_set = sdk.query(transfers_query + f" LIMIT {rows_limit}")


	if query_result_set:
		transfers_df = pd.DataFrame(query_result_set.rows, columns=query_result_set.columns)
		transfers_df['BLOCK_TIMESTAMP'] = pd.to_datetime(transfers_df['BLOCK_TIMESTAMP'])
		transfers_df['PROJECT_NAME'] = transfers_df['PROJECT_NAME'].fillna('other')
		return transfers_df

	else:
		return pd.DataFrame(columns=['BLOCK_TIMESTAMP', 'TX_HASH', 'EVENT_TYPE', 'NFT_ADDRESS',
       								'PROJECT_NAME', 'NFT_FROM_ADDRESS', 'NFT_TO_ADDRESS', 
									'TOKENID', 'SIDE'])


def load_df_bundle(wallet_address: str, start_date: str, rows_limit: int = 100_000):
	
	# use thread pool to speed up the loading of the dataframes
	with concurrent.futures.ThreadPoolExecutor() as executor:
		future_transactions = executor.submit(load_transactions, start_date=start_date, wallet_address=wallet_address, rows_limit=rows_limit)
		future_erc20_balances = executor.submit(load_erc20_balances, start_date=start_date, wallet_address=wallet_address, rows_limit=rows_limit)
		future_native_token_transfers = executor.submit(load_native_token_transfers, start_date=start_date, wallet_address=wallet_address, rows_limit=rows_limit)
		future_token_transfers = executor.submit(load_erc20_token_transfers, start_date=start_date, wallet_address=wallet_address, rows_limit=rows_limit)
		future_nft_sales = executor.submit(load_nft_sales, wallet_address.lower(), start_date, rows_limit=rows_limit)
		future_nft_transfers = executor.submit(load_nft_transfers, wallet_address.lower(), start_date, rows_limit=rows_limit)

	transactions_per_wallet = future_transactions.result()
	erc20_balances_per_wallet = future_erc20_balances.result()
	native_token_transfers_per_wallet = future_native_token_transfers.result()
	token_transfers_per_wallet = future_token_transfers.result()
	nft_sales_df = future_nft_sales.result()
	nft_transfers_df = future_nft_transfers.result()

	return transactions_per_wallet, erc20_balances_per_wallet, native_token_transfers_per_wallet, token_transfers_per_wallet, nft_sales_df, nft_transfers_df