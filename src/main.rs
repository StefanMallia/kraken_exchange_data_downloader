pub mod kraken_wsclient;
pub mod store_kraken_to_db;
use store_kraken_to_db::DbClient;
pub mod store_kraken_to_text;
pub mod kraken_message_handler;
pub mod message_types;
use std::env;

impl kraken_wsclient::Observer for kraken_message_handler::DbInsertQueue
{
    fn receive_message(&mut self, json_message: &json::JsonValue)
    {
        self.prepare_for_db_insert(json_message);
    }
}

impl kraken_wsclient::Observer for kraken_message_handler::TextFileInsertQueue
{
    fn receive_message(&mut self, json_message: &json::JsonValue)
    {
        self.prepare_for_text_insert(json_message);
    }
}

fn main()
{
    let db_credentials: std::string::String;
    let mut db_username: &str = "";
    let mut password: &str = "";
    let mut database_name: &str = "";
    let mut output_directory: &str = "";
    let mut db_credentials_path: &str = "";

    let args: Vec<String> = env::args().collect();
    //if !env::var("DB_USER").is_err() { }

    let mut temp_string1: std::string::String;
    let mut temp_string2: std::string::String;
    let mut temp_string3: std::string::String;
    if !env::var("DB_USER").is_err()
        && !env::var("DB_PASS").is_err()
        && !env::var("DB_NAME").is_err()
    {
        temp_string1 = env::var("DB_USER").unwrap();
        db_username = temp_string1.as_str();
        temp_string2 = env::var("DB_PASS").unwrap().clone();
        password = temp_string2.as_str();
        temp_string3 = env::var("DB_NAME").unwrap().clone();
        database_name = temp_string3.as_str();
    }
    for (arg_index, arg) in args.iter().enumerate()
    {
        if arg == "--dbname" { database_name = args[arg_index + 1].as_str() }
        if arg == "--dbuser" { db_username = args[arg_index + 1].as_str() }
        if arg == "--dbpassword" { password = args[arg_index + 1].as_str() }
        if arg == "--textoutputdirectory" { output_directory = args[arg_index + 1].as_str() }
        if arg == "--dbcredentialsfile" { db_credentials_path = args[arg_index + 1].as_str() }
        if arg == "--help" || (arg=="-h") { println!("Usage:\n--dbname DATABASE_NAME --dbuser DATABASEUSER --dbpassword DATABASEPASSWORD --dbcredentialsfile CREDENTIALSPATH --textoutputdirectory OUTPUTPATH"); }
    }

    if args.iter().any(|i| i == "--credentialsfile")
        && args.iter().any(|i| i == "--dbname")
    {
        db_credentials =
            std::fs::read_to_string(db_credentials_path
            ).expect("Something went wrong reading the file");
        let newline_index: usize = db_credentials.find("\n").unwrap();
        db_username = &db_credentials[0..newline_index];
        password = &db_credentials[newline_index..];
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

    let mut queues_vec: Vec<Box<dyn kraken_wsclient::Observer>> = vec![];
    if (args.iter().any(|i| i == "--credentialsfile")
            && args.iter().any(|i| i == "--dbname"))
       ||
       (args.iter().any(|i| i == "--dbname")
            && args.iter().any(|i| i == "--dbuser")
            && args.iter().any(|i| i == "--dbpassword"))
       ||
       (!env::var("DB_USER").is_err()
           && !env::var("DB_PASS").is_err()
           && !env::var("DB_NAME").is_err())
    {
        let mut db_client: store_kraken_to_db::TimescaleDbClient
            = store_kraken_to_db::
                TimescaleDbClient::new(database_name, db_username, password);

        for ticker in &asset_pairs_vec
        {
            db_client.create_trade_table(ticker.replace("/", "").as_str());
            db_client.create_depth_update_table(ticker.replace("/", "").as_str(), depth);
            db_client.create_spread_table(ticker.replace("/", "").as_str());
            db_client.create_orderbook_table(ticker.replace("/", "").as_str(), depth);

        }
        let db_insert_queue: Box<kraken_message_handler::DbInsertQueue>
            = Box::new(kraken_message_handler::DbInsertQueue::new(db_client,
                                                                  true));
        queues_vec.push(db_insert_queue);
    }
    if args.iter().any(|i| i == "--textoutputdirectory")
    {
        let mut text_writer: store_kraken_to_text::TextFile
            = store_kraken_to_text::TextFile::new(output_directory);
        let text_insert_queue: Box<kraken_message_handler::TextFileInsertQueue>
            = Box::new(kraken_message_handler::TextFileInsertQueue::new(text_writer));
        queues_vec.push(text_insert_queue);
    }
    kraken_wsclient::connect(asset_pairs_vec, subscriptions_vec,
                             queues_vec);
}