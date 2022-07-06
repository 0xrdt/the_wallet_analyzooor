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

for value in to_loop['eez_nft_transfers']:
    """print seperate statements for each value and key"""

    # distinct_query = "select distinct(" , value , ") from ethereum.core." , key
    statement_eez_nft_transfers = (f"select distinct({value}) from ethereum.core.eez_nft_transfers")
    print(statement_eez_nft_transfers)
    
for value in to_loop['ez_nft_mints']:
    """print seperate statements for each value and key"""

    # distinct_query = "select distinct(" , value , ") from ethereum.core." , key
    statement_ez_nft_mints = (f"select distinct({value}) from ethereum.core.ez_nft_mints")
    print(statement_ez_nft_mints)




    
