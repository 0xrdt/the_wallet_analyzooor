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
		if r.status_code == 400:
			return {}

		if r.status_code == 504:
			print("504 error, retrying...")
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

if __name__ == "__main__":
	import queries
	import flipside
	wallet_address='0xcf7a68127285c7c6c8546ce51b89d7e820f6d294'
	rows_limit=1
	chain_name = 'ethereum'
	query_template = queries.wallet_label
	query = query_template.\
		replace("$CHAIN_NAME", chain_name).\
		replace("$WALLET_ADDRESS", wallet_address.lower())
	data = flipside.get_data(query + f" LIMIT {rows_limit}")

	wallet_label = pd.DataFrame(data['results'], columns=data['columnLabels'])
	get_data(query)