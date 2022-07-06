

import requests
import json
import time

def get_data(sql_query: str):
	
	# load api key from json
	with open('flipside_api_key.json') as f:
		API_KEY = json.load(f)['api_key']

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
			if data: break
		except Exception as e:
			print(e)
			time.sleep(10)
			tries += 1

	return data




to_loop = {}
to_loop['ez_nft_mints'] = [
"MINT_TOKEN_SYMBOL",
"MINT_TOKEN_ADDRESS",
"NFT_ADDRESS",
"PROJECT_NAME",
"NFT_TO_ADDRESS",
"NFT_FROM_ADDRESS",
]
to_loop['eez_nft_transfers'] = [
"ORIGIN_TO_ADDRESS",
"ETH_FROM_ADDRESS",
"ETH_TO_ADDRESS",
"ORIGIN_FROM_ADDRESS",
"ORIGIN_TO_ADDRESS",
"IDENTIFIER"]
to_loop['ez_nft_sales'] = [
"PLATFORM_ADDRESS",
"PLATFORM_NAME",
'PROJECT_NAME',
"SELLER_ADDRESS",
"ORIGIN_TO_ADDRESS",
"ORIGIN_FROM_ADDRESS",
"NFT_ADDRESS",
"CURRENCY_ADDRESS",
"CURRENCY_SYMBOL",
"BUYER_ADDRESS"
]
# print(to_loop)

for value in to_loop['ez_nft_sales']:
    """print seperate statements for each value and key"""

    # distinct_query = "select distinct(" , value , ") from ethereum.core." , key
    statement_ez_nft_sales = (f"select distinct({value}) from ethereum.core.ez_nft_sales")
    
    print(statement_ez_nft_sales)
    sql_query = statement_ez_nft_sales
    data = get_data(sql_query)
    print(data)

for value in to_loop['eez_nft_transfers']:
    """print seperate statements for each value and key"""

    # distinct_query = "select distinct(" , value , ") from ethereum.core." , key
    statement_eez_nft_transfers = (f"select distinct({value}) from ethereum.core.eez_nft_transfers")
    print(statement_eez_nft_transfers)
    sql_query = statement_eez_nft_transfers
    data = get_data(sql_query)
    print(data)

for value in to_loop['ez_nft_mints']:
    """print seperate statements for each value and key"""

    # distinct_query = "select distinct(" , value , ") from ethereum.core." , key
    statement_ez_nft_mints = (f"select distinct({value}) from ethereum.core.ez_nft_mints")
    print(statement_ez_nft_mints)
    sql_query = statement_ez_nft_sales

    data = get_data(sql_query)
    print(data)


