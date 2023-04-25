import kaiko_lb_markets as kk

start_time = '2022-01-01T00:00:00.000Z'
end_time = '2023-04-18T00:00:00.000Z'
assets = ['usdc', 'usdt', 'eth', 'weth', 'btc', 'wbtc', 'dai']
protocols = ['aav2', 'cmpd', 'mkr']
apikey = 'your_api_key'
# To convert to fiat, specify currency in quote_assets.
quote_assets = ['usd']
# 1m, 1h, or 1d. Other values are not accepted
interval = '1d'

df = kk.kaiko_lb_converted(apikey, start_time=start_time, end_time=end_time, protocols=protocols, assets=assets,  quote_assets=quote_assets, interval=interval)
df.to_csv('lb_markets.csv')
