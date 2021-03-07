use std::collections::HashMap;
use crate::message_types::{KrakenMessage};
use crate::store_kraken_to_db;
use crate::store_kraken_to_text;

fn current_time() -> f64
{
    let start: std::time::SystemTime = std::time::SystemTime::now();
    let since_the_epoch: std::time::Duration
        = start.duration_since(std::time::UNIX_EPOCH)
        .expect("Time went backwards");
    since_the_epoch.as_secs_f64()
}

pub struct DbInsertQueue
{
    pub dict_of_tables: HashMap<String, Vec<Box<KrakenMessage>>>,
    pub db_client: store_kraken_to_db::DbClient,
    pub ignore_depth_ticks: bool
}
impl DbInsertQueue
{
    pub fn new(db_client: store_kraken_to_db::DbClient, ignore_depth_ticks: bool) -> DbInsertQueue
    {
        let dict_of_tables: HashMap<String, Vec<Box<KrakenMessage>>>
                = HashMap::new();
        DbInsertQueue{dict_of_tables, db_client, ignore_depth_ticks}
    }
    pub fn prepare_for_db_insert(&mut self, kraken_message: &json::JsonValue)
    {
        // let prices_vec: Vec<&str> = vec![];
        // let volumes_vec: Vec<&str> = vec![];
        // let timestamp_vec: Vec<&str> = vec![];
        // let local_times_vec: Vec<&str> = vec![];
        // let order_type_vec: Vec<&str> = vec![];
        // let misc_vec: Vec<&str> = vec![];
        // let index_in_message_vec: Vec<&str> = vec![];
        let local_time = current_time();
        if kraken_message["event"] == "heartbeat"
        {
            // println!("HEARTBEAT {}", kraken_message);
        }
        else if kraken_message[2] == "trade"
        {
            let ticker_name: String
                = str::replace(kraken_message[3].as_str().unwrap(), "/", ""
                               ).to_lowercase();
            let table_name_string
                = ["kraken_",
                   ticker_name.as_str(),
                   "_trade"].concat();
            let table_name = table_name_string.as_str();
            let kraken_msg_data: &json::JsonValue = &kraken_message[1];
            // println!("{}", kraken_msg_data.len());
            // println!("{}", kraken_message);
            for msg_index in 0..kraken_msg_data.len()
            {
                // let price: &str = kraken_msg_data[msg_index][0].as_str().unwrap();
                // let volume: &str = kraken_msg_data[msg_index][1].as_str().unwrap();
                // let timestamp: &str = kraken_msg_data[msg_index][3].as_str().unwrap();
                // let local_time: &str = current_time().to_string().as_str();
                let order_type: &str = kraken_msg_data[msg_index][4].as_str().unwrap();
                let order_type_bool: String;
                if order_type == "l"
                {
                    order_type_bool = String::from("TRUE");
                }
                else if order_type == "m"
                {
                    order_type_bool = String::from("FALSE");
                }
                else { order_type_bool = String::from("FALSE"); }
                let side: &str = kraken_msg_data[msg_index][3].as_str().unwrap();
                let side_bool: String;
                if side == "b"
                {
                    side_bool = String::from("TRUE");
                }
                else if side == "s"
                {
                    side_bool = String::from("FALSE");
                }
                else { side_bool = String::from("FALSE"); }
                // let misc: &str = kraken_msg_data[msg_index][5].as_str().unwrap();
                self.dict_of_tables.entry(String::from(table_name)).or_insert_with(|| vec![]);

                self.dict_of_tables.get_mut(table_name).unwrap()
                    .push(
                    Box::new(KrakenMessage::TradeMessage {
                        price: kraken_msg_data[msg_index][0].to_string(),
                        volume: kraken_msg_data[msg_index][1].to_string(),
                        timestamp: kraken_msg_data[msg_index][2].to_string(),
                        local_time: local_time.to_string(),
                        order_type: order_type_bool,
                        side: side_bool,
                        misc: kraken_msg_data[msg_index][5].to_string(),
                        index_in_message: msg_index.to_string()
                    })
                    );


            }
            // println!("{} {}", self.dict_of_tables[table_name].len(), table_name);
            //
            // println!("{}", self.dict_of_tables[table_name][0].local_time);
            if self.dict_of_tables[table_name].len() >= 10
            {
                let to_insert_then_remove = self.dict_of_tables.remove(table_name).unwrap();
                self.dict_of_tables.insert(table_name.to_string(), vec![]);

                self.db_client.insert_trades(ticker_name.as_str(),
                                             &to_insert_then_remove);
            }
        }
        else if (
                     (kraken_message[1]["as"].is_null()
                     & ! kraken_message[1]["a"].is_null())
                     |
                     (kraken_message[1]["bs"].is_null()
                     & ! kraken_message[1]["b"].is_null())
                )
                &
                !self.ignore_depth_ticks
        {
            // println!("{}", kraken_message);
            let message_type_ndx;
            let ticker_name_ndx;
            let both_a_and_b_ndx;
            if kraken_message.len() == 4
            {
                //either bid or ask data included
                message_type_ndx = 2;
                ticker_name_ndx = 3;
                both_a_and_b_ndx = 2;
            }
            else //==5
            {
                //both bid and ask data included
                message_type_ndx = 3;
                ticker_name_ndx = 4;
                both_a_and_b_ndx = 3;
            }
            let ticker_name: String
                = str::replace(kraken_message[ticker_name_ndx].as_str().unwrap()
                               , "/", ""
                              ).to_lowercase();
            //kraken_message[2][5..] is the depth specified on subscription
            //each message will contain something like book-100 where
            //100 is the number of price levels
            let depth = &kraken_message[message_type_ndx]
                .as_str().unwrap()
                .to_string()[5..];
            let table_name_string
                =
                ["kraken_",
                ticker_name.as_str(),
                "_depth_update_",
                depth
                ].concat();
            let table_name = table_name_string.as_str();

            //Two example messages:
            // [1234,
            //  { "a": [
            //             ["5541.30000","2.50700000","1534614248.456738"],
            //             ["5542.50000","0.40100000","1534614248.456738"]
            //         ]
            //  },
            //  {"b": [["5541.30000","0.00000000","1534614335.345903"]]},
            //  "book-10",
            //  "XBT/USD"
            // ]
            // [1234,
            //  {"a": [["5541.30000", "2.50700000", "1534614248.456738", "r"],
            //         ["5542.50000", "0.40100000","1534614248.456738", "r"]
            //        ]
            //  },
            //  "book-25",
            //  "XBT/USD"
            // ]

            for kraken_msg_ndx in 1..both_a_and_b_ndx
            {

                let ask_or_bid;
                if kraken_message[kraken_msg_ndx]["a"].is_null()
                {
                    ask_or_bid = "b"
                }
                else
                {
                    ask_or_bid = "a"
                }
            let kraken_msg_data = &kraken_message[kraken_msg_ndx][ask_or_bid];

            for msg_index in 0..kraken_msg_data.len()
            {
                let is_republish;
                if kraken_msg_data[msg_index].len() == 4
                {is_republish = String::from("TRUE")}
                else
                {is_republish = String::from("FALSE")}
                let is_ask;
                if ask_or_bid == "a" {is_ask = String::from("TRUE")}
                else {is_ask = String::from("FALSE")}
                // let misc: &str = kraken_msg_data[msg_index][5].as_str().unwrap();
                self.dict_of_tables.entry(String::from(table_name)
                                        ).or_insert_with(|| vec![]);

                // println!("{}", kraken_msg_data[msg_index][0].to_string());
                self.dict_of_tables.get_mut(table_name).unwrap()
                    .push(
                        Box::new(KrakenMessage::DepthMessage{price: kraken_msg_data[msg_index][0].to_string(),
                            volume: kraken_msg_data[msg_index][1].to_string(),
                            is_republish: is_republish,
                            timestamp: kraken_msg_data[msg_index][2].to_string(),
                            local_time: local_time.to_string(),
                            is_ask: is_ask,
                            index_in_message: msg_index.to_string()
                        })
                    );

            }

            }
            if self.dict_of_tables[table_name].len() >= 1000
            {
                let to_insert_then_remove = &self.dict_of_tables.remove(table_name).unwrap();
                self.dict_of_tables.insert(table_name.to_string(), vec![]);

                self.db_client.insert_depth_update(ticker_name.as_str(),
                                                   &to_insert_then_remove,
                                                   depth);
            }
        }
        else if kraken_message[2] == "spread"
        {
            let ticker_name: String
                = str::replace(kraken_message[3].as_str().unwrap(), "/", ""
                               ).to_lowercase();
            let table_name_string
                = ["kraken_spread_",
                   ticker_name.as_str()].concat();
            let table_name = table_name_string.as_str();
            let kraken_msg_data: &json::JsonValue = &kraken_message[1];
            // println!("{}", kraken_msg_data.len());
            // println!("{}", kraken_message);
            self.dict_of_tables.entry(String::from(table_name)).or_insert_with(|| vec![]);

            self.dict_of_tables.get_mut(table_name).unwrap()
                .push(
                Box::new(KrakenMessage::SpreadMessage {
                    bid: kraken_msg_data[0].to_string(),
                    ask: kraken_msg_data[1].to_string(),
                    bid_volume: kraken_msg_data[3].to_string(),
                    ask_volume: kraken_msg_data[4].to_string(),
                    timestamp: kraken_msg_data[2].to_string(),
                    local_time: local_time.to_string()
                    })
                );

            if self.dict_of_tables[table_name].len() >= 10
            {
                let to_insert_then_remove = self.dict_of_tables.remove(table_name).unwrap();
                self.dict_of_tables.insert(table_name.to_string(), vec![]);

                self.db_client.insert_spreads(ticker_name.as_str(),
                                             &to_insert_then_remove);
            }
        }
        else if (! kraken_message[1]["as"].is_null()
            & kraken_message[1]["a"].is_null())
            |
            (! kraken_message[1]["bs"].is_null()
                & kraken_message[1]["b"].is_null())
        {
            let message_type_ndx = 2;
            let ticker_name_ndx = 3;

            let ticker_name: String
                = str::replace(kraken_message[ticker_name_ndx].as_str().unwrap()
                               , "/", ""
            ).to_lowercase();
            //kraken_message[2][5..] is the depth specified on subscription
            //each message will contain something like book-100 where
            //100 is the number of price levels
            let depth = &kraken_message[message_type_ndx]
                .as_str().unwrap()
                .to_string()[5..];
            let table_name_string
                =
                ["kraken_",
                    ticker_name.as_str(),
                    "_orderbook_",
                    depth
                ].concat();
            let table_name = table_name_string.as_str();

            // [0,
            //  {
            //   "as": [["5541.30000","2.50700000","1534614248.123678"],
            //          ["5541.80000","0.33000000","1534614098.345543"],
            //          ["5542.70000", "0.64700000","1534614244.654432"]
            //         ],
            //  "bs": [["5541.20000","1.52900000","1534614248.765567"],
            //         ["5539.90000","0.30000000", "1534614241.769870"],
            //         ["5539.50000","5.00000000","1534613831.243486"]
            //        ]
            //  },
            //   "book-100",
            //   "XBT/USD"
            //  ]

            for side_of_orders in ["as", "bs"].iter()
            {

                let kraken_msg_data = &kraken_message[1][*side_of_orders];

                for msg_index in 0..kraken_msg_data.len()
                {
                    let is_ask;
                    if *side_of_orders == String::from("as") {is_ask = String::from("TRUE")}
                    else {is_ask = String::from("FALSE")}
                    self.dict_of_tables.entry(String::from(table_name)
                    ).or_insert_with(|| vec![]);


                    self.dict_of_tables.get_mut(table_name).unwrap()
                        .push(
                            Box::new(KrakenMessage::OrderBookMessage{price: kraken_msg_data[msg_index][0].to_string(),
                                volume: kraken_msg_data[msg_index][1].to_string(),
                                timestamp: kraken_msg_data[msg_index][2].to_string(),
                                local_time: local_time.to_string(),
                                is_ask: is_ask,
                                index_in_message: msg_index.to_string()
                            })
                        );

                }
            }

            if self.dict_of_tables[table_name].len() > 0
            {
                let to_insert_then_remove = &self.dict_of_tables.remove(table_name).unwrap();
                self.dict_of_tables.insert(table_name.to_string(), vec![]);


                self.db_client.insert_orderbook(ticker_name.as_str(),
                                                &to_insert_then_remove,
                                                depth);
            }
        }
        else
        {

        }
    }
}

pub struct TextFileInsertQueue
{
    pub dict_of_files: HashMap<String, Vec<Box<json::JsonValue>>>,
    pub text_file_writer: store_kraken_to_text::TextFile
}
impl TextFileInsertQueue
{
    pub fn new(text_file_writer: store_kraken_to_text::TextFile) -> TextFileInsertQueue
    {
        let dict_of_files: HashMap<String, Vec<Box<json::JsonValue>>>
            = HashMap::new();
        TextFileInsertQueue{dict_of_files, text_file_writer}
    }
    pub fn prepare_for_text_insert(&mut self, kraken_message: &json::JsonValue)
    {
        let local_time = current_time();
        if kraken_message["event"] == "heartbeat"
        {
            // println!("HEARTBEAT {}", kraken_message);
        }
        else if kraken_message[2] == "trade"
        {
            let ticker_name: String
                = str::replace(kraken_message[3].as_str().unwrap(), "/", ""
            ).to_lowercase();
            let file_name_string
                = ["kraken_",
                ticker_name.as_str(),
                "_trade.json"].concat();
            let file_name = file_name_string.as_str();
            self.dict_of_files.entry(String::from(file_name)).or_insert_with(|| vec![]);

            let mut kraken_message_local_time:json::JsonValue = kraken_message.clone();
            kraken_message_local_time.push(local_time);

            self.dict_of_files.get_mut(file_name).unwrap()
                .push(Box::new(kraken_message_local_time));

            if self.dict_of_files[file_name].len() > 20
            {
                let to_insert_then_remove = &self.dict_of_files.remove(file_name).unwrap();
                self.dict_of_files.insert(file_name.to_string(), vec![]);
                self.text_file_writer.append_to_file(file_name, &to_insert_then_remove);
            }

        }
        else if (kraken_message[1]["as"].is_null()
            & ! kraken_message[1]["a"].is_null())
            |
            (kraken_message[1]["bs"].is_null()
                & ! kraken_message[1]["b"].is_null())
        {
            // println!("{}", kraken_message);
            let message_type_ndx;
            let ticker_name_ndx;
            let both_a_and_b_ndx;
            if kraken_message.len() == 4
            {
                //either bid or ask data included
                message_type_ndx = 2;
                ticker_name_ndx = 3;
                both_a_and_b_ndx = 2;
            }
            else //==5
            {
                //both bid and ask data included
                message_type_ndx = 3;
                ticker_name_ndx = 4;
                both_a_and_b_ndx = 3;
            }
            let ticker_name: String
                = str::replace(kraken_message[ticker_name_ndx].as_str().unwrap()
                               , "/", ""
            ).to_lowercase();
            //kraken_message[2][5..] is the depth specified on subscription
            //each message will contain something like book-100 where
            //100 is the number of price levels
            let depth = &kraken_message[message_type_ndx]
                .as_str().unwrap()
                .to_string()[5..];
            let file_name_string
                =
                ["kraken_",
                    ticker_name.as_str(),
                    "_depth_update_",
                    depth, ".json"
                ].concat();
            let file_name = file_name_string.as_str();
            //Two example messages:
            // [1234,
            //  { "a": [
            //             ["5541.30000","2.50700000","1534614248.456738"],
            //             ["5542.50000","0.40100000","1534614248.456738"]
            //         ]
            //  },
            //  {"b": [["5541.30000","0.00000000","1534614335.345903"]]},
            //  "book-10",
            //  "XBT/USD"
            // ]
            // [1234,
            //  {"a": [["5541.30000", "2.50700000", "1534614248.456738", "r"],
            //         ["5542.50000", "0.40100000","1534614248.456738", "r"]
            //        ]
            //  },
            //  "book-25",
            //  "XBT/USD"
            // ]
            self.dict_of_files.entry(String::from(file_name)).or_insert_with(|| vec![]);

            let mut kraken_message_local_time:json::JsonValue = kraken_message.clone();
            kraken_message_local_time.push(local_time);

            self.dict_of_files.get_mut(file_name).unwrap()
                .push(Box::new(kraken_message_local_time));

            if self.dict_of_files[file_name].len() > 200
            {
                let to_insert_then_remove = &self.dict_of_files.remove(file_name).unwrap();
                self.dict_of_files.insert(file_name.to_string(), vec![]);
                self.text_file_writer.append_to_file(file_name, &to_insert_then_remove);
            }

        }
        else if kraken_message[2] == "spread"
        {
            let ticker_name: String
                = str::replace(kraken_message[3].as_str().unwrap(), "/", ""
            ).to_lowercase();
            let file_name_string
                = ["kraken_spread_",
                ticker_name.as_str(), ".json"].concat();
            let file_name = file_name_string.as_str();

            self.dict_of_files.entry(String::from(file_name)).or_insert_with(|| vec![]);

            let mut kraken_message_local_time:json::JsonValue = kraken_message.clone();
            kraken_message_local_time.push(local_time);

            self.dict_of_files.get_mut(file_name).unwrap()
                .push(Box::new(kraken_message_local_time));

            if self.dict_of_files[file_name].len() > 20
            {
                let to_insert_then_remove = &self.dict_of_files.remove(file_name).unwrap();
                self.dict_of_files.insert(file_name.to_string(), vec![]);
                self.text_file_writer.append_to_file(file_name, &to_insert_then_remove);
            }

        }
        else if (! kraken_message[1]["as"].is_null()
            & kraken_message[1]["a"].is_null())
            |
            (! kraken_message[1]["bs"].is_null()
                & kraken_message[1]["b"].is_null())
        {
            let message_type_ndx = 2;
            let ticker_name_ndx = 3;

            let ticker_name: String
                = str::replace(kraken_message[ticker_name_ndx].as_str().unwrap()
                               , "/", ""
            ).to_lowercase();
            //kraken_message[2][5..] is the depth specified on subscription
            //each message will contain something like book-100 where
            //100 is the number of price levels
            let depth = &kraken_message[message_type_ndx]
                .as_str().unwrap()
                .to_string()[5..];
            let file_name_string
                =
                ["kraken_",
                    ticker_name.as_str(),
                    "_orderbook_",
                    depth, ".json"
                ].concat();
            let file_name = file_name_string.as_str();

            // [0,
            //  {
            //   "as": [["5541.30000","2.50700000","1534614248.123678"],
            //          ["5541.80000","0.33000000","1534614098.345543"],
            //          ["5542.70000", "0.64700000","1534614244.654432"]
            //         ],
            //  "bs": [["5541.20000","1.52900000","1534614248.765567"],
            //         ["5539.90000","0.30000000", "1534614241.769870"],
            //         ["5539.50000","5.00000000","1534613831.243486"]
            //        ]
            //  },
            //   "book-100",
            //   "XBT/USD"
            //  ]

            self.dict_of_files.entry(String::from(file_name)).or_insert_with(|| vec![]);

            let mut kraken_message_local_time:json::JsonValue = kraken_message.clone();
            kraken_message_local_time.push(local_time);

            self.dict_of_files.get_mut(file_name).unwrap()
                .push(Box::new(kraken_message_local_time));

            if self.dict_of_files[file_name].len() > 0
            {
                let to_insert_then_remove = &self.dict_of_files.remove(file_name).unwrap();
                self.dict_of_files.insert(file_name.to_string(), vec![]);
                self.text_file_writer.append_to_file(file_name, &to_insert_then_remove);
            }

        }
        else
        {

        }
    }
}