# DeFi Lending & Borrowing Data, by Kaiko

### A python module, for Kaiko's DeFi data
An open-source Python module (`kaiko_lb_markets`) that offers a simple and intuitive interface for working with Kaiko's Lending &amp; Borrowing data.

## Installation 
The module containing all the functions is named `kaiko_lb_markets`. To use its functions, place it in your working directory and import it into your scripts. 

```python 
import kaiko_lb_markets as kk
```
## Example
A sample script is included in this repository and named `example.py`. It demonstrates how to use each function within the `kaiko_lb_markets` module.

### Example 1: Retrieving DeFi Lending & Borrowing Rates and Liquidity Data
```python 
import kaiko_lb_markets as kk

start_time = '2022-03-01T00:00:00.000Z'
end_time = '2023-04-18T00:00:00.000Z'
assets = ['usdc', 'usdt', 'eth', 'weth', 'btc', 'wbtc', 'dai']
protocols = ['aav2', 'cmpd', 'mkr']
apikey = 'your_kaiko_api_key_here'
# To convert to fiat, specify currency in quote_assets.
quote_assets = ['usd']
interval = '1d'

df = kk.kaiko_lb_markets(apikey, start_time=start_time, end_time=end_time, protocols=protocols, assets=assets,  quote_assets=quote_assets, interval=interval)
df.to_csv('lb_markets.csv')
```
