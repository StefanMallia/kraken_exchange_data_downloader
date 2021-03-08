# Kraken exchange tick data downloader
Connect to Kraken's websocket api to download real time orderbook and trade data. Can download to text files or to a Postres database with timescaledb extension.
Example usage:
mkdir output; nohup target/release/kraken_exchange_data_downloader --textoutputdirectory output > kraken_download.log 2>&1 &
