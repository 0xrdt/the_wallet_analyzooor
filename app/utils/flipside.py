import requests
import json
import time
import os

# get env variable API_KEY
API_KEY = os.environ.get('FLIPSIDE_API_KEY', None)

if not API_KEY:
	try:
		# load api key from json
		with open('flipside_api_key.json') as f:
			API_KEY = json.load(f)['api_key']
	except Exception as e:
		print(e)
		print("Error loading api key, please export the FLIPSIDE_API_KEY environment variable,"
			  "create a file named flipside_api_key.json with" 
			  "your api key in it or set flipside.API_KEY to your key")


def get_data(sql_query: str):
	
	
	def create_query(sql_query: str):
		TTL_MINUTES = 15
		
		r = requests.post(
			'https://node-api.flipsidecrypto.com/queries', 
			data=json.dumps({
				"sql": sql_query,
				"ttlMinutes": TTL_MINUTES
			}),
			headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": API_KEY},
		)
		if r.status_code != 200:
			raise Exception("Error creating query, got response: " + r.text + "with status code: " + str(r.status_code))
		
		return json.loads(r.text)    

	def get_query_results(token):
		r = requests.get(
			'https://node-api.flipsidecrypto.com/queries/' + token, 
			headers={"Accept": "application/json", "Content-Type": "application/json", "x-api-key": API_KEY}
		)
		if r.status_code == 400:
			print("No rows found, returning empty dict")
			return {}

		if (r.status_code == 504) or (r.status_code == 502):
			print(f"{r.status_code} error, retrying...")
			time.sleep(10)
			return get_query_results(token)

		if r.status_code != 200:
			raise Exception("Error getting query results, got response: " + r.text + "with status code: " + str(r.status_code))
		
		data = json.loads(r.text)
		if data['status'] == 'running':
			time.sleep(10)
			return get_query_results(token)

		return data
	
	query = create_query(sql_query)

	token = query.get('token')

	tries = 0
	while tries <= 10:
		try:
			data = get_query_results(token)
			return data
		except Exception as e:
			print(e)
			time.sleep(10)
			tries += 1

	return data

def get_data_safe(sql_query: str):
	safety_check_query = f"with tmp_table as ({sql_query}) select count(*) from tmp_table limit 1"

	data = get_data(safety_check_query)

	if data['results'][0][0] == 0:
		print("No rows found, returning empty dict")
		return {}

	else:
		return get_data(sql_query)


if __name__ == "__main__":
	import pandas as pd
	import queries
	import flipside
	def load_transactions(wallet_address: str, start_date: str, rows_limit: int = 100_000, chain_name='ethereum'):
		query_template = queries.transactions_per_wallet
		query = query_template.\
			replace("$CHAIN_NAME", chain_name).\
			replace("$START_DATE", start_date).\
			replace("$WALLET_ADDRESS", wallet_address.lower())
		data = flipside.get_data_safe(query + f" LIMIT {rows_limit}")

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


	start_date = '2022-02-01'
	wallet_address = '0xAfB5853Ba41eE4d8886eD66793535218bEc8F1A9'
	tmp_df = load_transactions(wallet_address, start_date, chain_name='arbitrum')