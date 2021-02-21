mod kraken_wsclient;
mod store_kraken_to_db;
mod kraken_message_handler;
mod message_types;
use std::env;

impl kraken_wsclient::Observer for kraken_message_handler::DbInsertQueue
{
    fn receive_message(&mut self, json_message: &json::JsonValue)
    {
        self.prepare_for_db_insert(json_message);
    }
}
fn main()
{
    let db_credentials: std::string::String;
    let mut db_username: Option<&str> = None;
    let mut password: Option<&str> = None;
    let mut database_name: Option<&str> = None;

    let args: Vec<String> = env::args().collect();
    if args.iter().any(|i| i=="--help")
        || args.iter().any(|i| i=="-h")
    {
        println!("Usage:\n--dbname DATABASE_NAME --dbuser DATABASEUSER --dbpassword DATABASEPASSWORD");
    }
    else {
        if args.len() >= 7 && args.iter().any(|i| i == "--dbname")
            && args.iter().any(|i| i == "--dbuser")
            && args.iter().any(|i| i == "--dbpassword")
        {
            for (arg_index, arg) in args.iter().enumerate()
            {
                if arg == "--dbname" { database_name = Some(args[arg_index + 1].as_str()) }
                if arg == "--dbuser" { db_username = Some(args[arg_index + 1].as_str()) }
                if arg == "--dbpassword" { password = Some(args[arg_index + 1].as_str()) }
            }
        } else {
            println!("One of 'dbname', 'dbuser', 'dbpassword' not specified. Using default database name 'krakenexchange' and reading database credentials file 'postgresql'");
            db_credentials =
                std::fs::read_to_string("./postgresql"
                ).expect("Something went wrong reading the file");
            let newline_index: usize = db_credentials.find("\n").unwrap();
            db_username = Some(&db_credentials[0..newline_index]);
            password = Some(&db_credentials[newline_index..]);
            database_name = Some("krakenexchange");
        }
        let depth: &str = "25";
        let asset_pairs_vec: std::vec::Vec<String> = vec!["EUR/CAD",
                                                          "EUR/CHF", "EUR/GBP", "EUR/JPY", "USD/CHF",
                                                          "XBT/USDT", "ETH/XBT",
                                                          "ETH/CAD", "ETH/EUR", "ETH/GBP", "ETH/JPY", "ETH/USD",
                                                          "XBT/EUR", "XBT/USD",
                                                          "XMR/XBT", "XMR/EUR", "XMR/USD", "EUR/USD", "GBP/USD", "USD/CAD", "USD/JPY"
        ].into_iter().map(String::from).collect();
        let subscriptions_vec: std::vec::Vec<String> = vec!["{\"name\": \"trade\"}",
                                                            ["{\"name\": \"book\", \"depth\": ", depth, "}"].concat().as_str()
                      
        ].into_iter().map(String::from).collect(); //"{\"name\": \"spread\"}"

        let mut db_client: store_kraken_to_db::DbClient
            = store_kraken_to_db::
        DbClient::new(database_name.unwrap(), db_username.unwrap(), password.unwrap());
        for ticker in &asset_pairs_vec
        {
            db_client.create_trade_table(ticker.replace("/", "").as_str());
            //db_client.create_depth_update_table(ticker.replace("/", "").as_str(), depth);
            //db_client.create_spread_table(ticker.replace("/", "").as_str());
            db_client.create_orderbook_table(ticker.replace("/", "").as_str(), depth);
        }
        let db_insert_queue: Box<kraken_message_handler::DbInsertQueue>
            = Box::new(kraken_message_handler::DbInsertQueue::new(db_client));
        kraken_wsclient::connect(asset_pairs_vec, subscriptions_vec,
                                 vec![db_insert_queue]);
    }
}