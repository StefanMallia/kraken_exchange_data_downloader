use postgres::{Client, NoTls};
use crate::message_types::{KrakenMessage};

pub struct DbClient
{
    pub db_name: String,
    pub client: Client,
}

impl DbClient
{
    pub fn new(db_name: &str, username: &str, password: &str) -> DbClient
    {
        let mut client =
            Client::connect(["dbname= ", db_name," host=localhost user=",
                             username, " password=", password
                            ].join("").as_str(),
                            NoTls).unwrap();
        client.batch_execute(
            "CREATE EXTENSION IF NOT EXISTS timescaledb;"

        ).unwrap();

        println!("Connected to database.");
        DbClient{db_name: db_name.to_string(), client: client}
    }

    pub fn create_trade_table(&mut self, ticker_name: &str)
    {
        let table_name = ["kraken_trade_" ,
                          ticker_name.to_lowercase().as_str()].join("");
        let table_name = table_name.as_str();

        self.client.batch_execute(
            ["CREATE TABLE IF NOT EXISTS ", table_name, " (
                row_id                SMALLSERIAL,
                price                 DECIMAL,
                volume                DECIMAL,
                timestamp             TIMESTAMP WITH TIME ZONE,
                local_time            TIMESTAMP WITH TIME ZONE,
                side                  BOOL,
                order_type            BOOL,
                misc                  VARCHAR,
                index_in_message      SMALLINT,
                PRIMARY KEY (row_id, local_time)
            );
            SELECT create_hypertable('", table_name,"', 'local_time',
                                     if_not_exists => TRUE);
            ALTER SEQUENCE ", table_name, "_row_id_seq CYCLE;"
            ].join("").as_str()
        ).unwrap();
    }

    pub fn insert_trades(&mut self, ticker_name: &str,
                         trade_messages_vec: &Vec<Box<KrakenMessage>>)
    {
        let mut values_string: String = String::new();

        for (_x_index, trade_message) in trade_messages_vec.iter().enumerate()
        {
            match &**trade_message
            {
                KrakenMessage::TradeMessage{price, volume,
                                            timestamp, local_time,
                                            side, order_type,
                                            misc, index_in_message}
                =>
                    values_string = [values_string.as_str(), "(",
                                     price.as_str(), ", ",
                                     volume.as_str(), ", to_timestamp(",
                                     timestamp.as_str(), "), to_timestamp(",
                                     local_time.as_str(), "), ",
                                     side.as_str(), ", ",
                                     order_type.as_str(), ", '",
                                     misc.as_str(), "', ",
                                     index_in_message.as_str(),
                                   "), "
                                  ].concat(),
                _ => println!("error")
            }
        }
        values_string.truncate(values_string.len()-2);
        println!("Inserted trade: {}", ticker_name);
        self.client.batch_execute(
            ["INSERT INTO kraken_trade_", ticker_name, "
            (price, volume, timestamp,
             local_time, side, order_type, misc, index_in_message)
            VALUES ",
            values_string.as_str(),
            ";"
            ].join("").as_str()
        ).unwrap();
    }

    pub fn create_depth_update_table(&mut self, ticker_name: &str, depth: &str)
    {
        {
            let table_name = ["kraken_depth_update_", depth, "_" ,
                ticker_name.to_lowercase().as_str()].join("");
            let table_name = table_name.as_str();
            self.client.batch_execute(
                ["CREATE TABLE IF NOT EXISTS ", table_name, " (
                row_id                SMALLSERIAL,
                price                 DECIMAL,
                volume                DECIMAL,
                is_republish          BOOL,
                timestamp             TIMESTAMP WITH TIME ZONE,
                local_time            TIMESTAMP WITH TIME ZONE,
                index_in_message      SMALLINT,
                is_ask                BOOL,
                PRIMARY KEY (row_id, local_time)
            );
            SELECT create_hypertable('", table_name,"', 'local_time',
                                     if_not_exists => TRUE);
            ALTER SEQUENCE ", table_name, "_row_id_seq CYCLE;"
                ].join("").as_str()
            ).unwrap();
        };
    }
    
    pub fn create_spread_table(&mut self, ticker_name: &str)
    {
        {
            let table_name = ["kraken_spread_",
                ticker_name.to_lowercase().as_str()].join("");
            let table_name = table_name.as_str();
            self.client.batch_execute(
                ["CREATE TABLE IF NOT EXISTS ", table_name, " (
                row_id                SMALLSERIAL,
                bid                   DECIMAL,
                ask                   DECIMAL,
                bid_volume            DECIMAL,
                ask_volume            DECIMAL,
                timestamp             TIMESTAMP WITH TIME ZONE,
                local_time            TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (row_id, local_time)
            );
            SELECT create_hypertable('", table_name,"', 'local_time',
                                     if_not_exists => TRUE);
            ALTER SEQUENCE ", table_name, "_row_id_seq CYCLE;"
                ].join("").as_str()
            ).unwrap();
        };
    }

    pub fn create_orderbook_table(&mut self, ticker_name: &str, depth: &str)
    {
        {
            let table_name = ["kraken_orderbook_", depth, "_" ,
                ticker_name.to_lowercase().as_str()].join("");
            let table_name = table_name.as_str();
            self.client.batch_execute(
                ["CREATE TABLE IF NOT EXISTS ", table_name, " (
                row_id                SMALLSERIAL,
                price                 DECIMAL,
                volume                DECIMAL,
                timestamp             TIMESTAMP WITH TIME ZONE,
                local_time            TIMESTAMP WITH TIME ZONE,
                index_in_message      SMALLINT,
                is_ask                BOOL,
                PRIMARY KEY (row_id, local_time)
            );
            SELECT create_hypertable('", table_name,"', 'local_time',
                                     if_not_exists => TRUE);
            ALTER SEQUENCE ", table_name, "_row_id_seq CYCLE;"
                ].join("").as_str()
            ).unwrap();
        };
    }

    pub fn insert_depth_update(&mut self, ticker_name: &str,
                               depth_messages_vec: &Vec<Box<KrakenMessage>>,
                               depth: &str)
    {
        let mut values_string: String = String::new();

        for (_x_index, depth_message) in depth_messages_vec.iter().enumerate()
        {
            match &**depth_message
            {
                KrakenMessage::DepthMessage{price, volume, is_republish, timestamp,
                    local_time, index_in_message, is_ask} =>
                // let depth_message = kraken_message.DepthMessage;
                    values_string = [values_string.as_str(), "(",
                                     price.as_str(), ", ",
                                     volume.as_str(), ", ",
                                     is_republish.as_str(), ", to_timestamp(",
                                     timestamp.as_str(), "), to_timestamp(",
                                     local_time.as_str(), "), ",
                                     index_in_message.as_str(), ", ",
                                     is_ask.as_str(),
                                     "), "
                                    ].concat(),
                _ => println!("error")
            }
        }
        values_string.truncate(values_string.len()-2);
        println!("Inserted depth: {}", ticker_name);
        self.client.batch_execute(
            ["INSERT INTO kraken_depth_update_", depth, "_",  ticker_name, "
            (price, volume, is_republish, timestamp,
             local_time, index_in_message, is_ask)
            VALUES ",
                values_string.as_str(),
                ";"
            ].join("").as_str()
        ).unwrap();
    }
    
    pub fn insert_spreads(&mut self, ticker_name: &str,
                               spread_messages_vec: &Vec<Box<KrakenMessage>>)
    {
         let mut values_string: String = String::new();
        
         for (_x_index, spread_message) in spread_messages_vec.iter().enumerate()
         {
             match &**spread_message
             {
                 KrakenMessage::SpreadMessage{bid, ask, bid_volume, ask_volume,
                                              timestamp, local_time} =>
                     values_string = [values_string.as_str(), "(",
                                      bid.as_str(), ", ",
                                      ask.as_str(), ", ",
                                      bid_volume.as_str(), ", ",
                                      ask_volume.as_str(), ", to_timestamp(",
                                      timestamp.as_str(), "), to_timestamp(",
                                      local_time.as_str(), ") ",
                                      "), "
                                     ].concat(),
                 _ => println!("error")
             }
         }
         values_string.truncate(values_string.len()-2);
         println!("Inserted spread: {}", ticker_name);
         self.client.batch_execute(
             ["INSERT INTO kraken_spread_",  ticker_name, "
             (bid, ask, bid_volume, ask_volume, timestamp,
              local_time)
             VALUES ",
                 values_string.as_str(),
                 ";"
             ].join("").as_str()
         ).unwrap();
    }

    pub fn insert_orderbook(&mut self, ticker_name: &str,
                            orderbook_messages_vec: &Vec<Box<KrakenMessage>>,
                            depth: &str)
    {
        let mut values_string: String = String::new();
        for (_x_index, orderbook_message) in orderbook_messages_vec.iter().enumerate()
        {
            match &**orderbook_message
            {
                KrakenMessage::OrderBookMessage{price, volume, timestamp,
                    local_time, index_in_message, is_ask} =>
                // let depth_message = kraken_message.DepthMessage;
                    values_string = [values_string.as_str(), "(",
                        price.as_str(), ", ",
                        volume.as_str(), ", to_timestamp(",
                        timestamp.as_str(), "), to_timestamp(",
                        local_time.as_str(), "), ",
                        index_in_message.as_str(), ", ",
                        is_ask.as_str(),
                        "), "
                    ].concat(),
                //TODO throw an error here
                _ => println!("error")
            }
        }
        println!("Inserted orderbook: {}", ticker_name);
        values_string.truncate(values_string.len()-2);
        self.client.batch_execute(
            ["INSERT INTO kraken_orderbook_", depth, "_",  ticker_name, "
            (price, volume, timestamp,
             local_time, index_in_message, is_ask)
            VALUES ",
                values_string.as_str(),
                ";"
            ].join("").as_str()
        ).unwrap();
    }
}