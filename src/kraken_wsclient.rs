use ws;
use json;
use url;

#[path="kraken_message_handler.rs"] mod kraken_message_handler;

pub trait Observer
{
    fn receive_message(&mut self, json_message: &json::JsonValue);
}





struct MyFactory
{
    observers: Vec<Box<dyn Observer>>,
    asset_pairs_vec: std::vec::Vec<String>,
    subscriptions_vec: std::vec::Vec<String>
}

impl ws::Factory for MyFactory
{
    type Handler = Client;


    fn connection_made(&mut self, ws_out: ws::Sender) -> Client
    {
        Client{ws_out: ws_out, observers: to_use_observers,
            asset_pairs_vec: asset_pairs_vec.clone(),
            subscriptions_vec: subscriptions_vec.clone(),
            last_update: std::time::Instant::now()}
    }
}

pub fn connect(asset_pairs_vec: std::vec::Vec<String>,
               subscriptions_vec: std::vec::Vec<String>,//,
               mut observers: Vec<Box<dyn Observer>>)
{

    //: fn(ws::Sender) -> Client
    let factory =
        move |ws_out: ws::Sender|
            {
                println!("Connected to websocket.");
                // work around for transferring ownership of Observers from this closure
                // to Client
                let mut to_use_observers: Vec<Box<dyn Observer>> = vec![];
                while observers.len() > 0
                {
                    to_use_observers.push(observers.pop().unwrap());
                }
                Client {
                    ws_out: ws_out,
                    observers: to_use_observers,
                    asset_pairs_vec: asset_pairs_vec.clone(),
                    subscriptions_vec: subscriptions_vec.clone(),
                    last_update: std::time::Instant::now()
                }
            };
    let settings = ws::Settings {
        ..Default::default()
    };
    let mut websocket = ws::Builder::new().with_settings(settings).build(factory).unwrap();


    websocket.connect(url::Url::parse("wss://ws.kraken.com").unwrap());
    websocket.run();
}



// pub fn connect(asset_pairs_vec: std::vec::Vec<String>,
//                subscriptions_vec: std::vec::Vec<String>,//,
//                mut observers: Vec<Box<dyn Observer>>)
// {
//
//     //: fn(ws::Sender) -> Client
//     let factory =
//         move |ws_out: ws::Sender|
//         {
//             println!("Connected to websocket.");
//             // work around for transferring ownership of Observers from this closure
//             // to Client
//             let mut to_use_observers: Vec<Box<dyn Observer>> = vec![];
//             while observers.len() > 0
//             {
//                 to_use_observers.push(observers.pop().unwrap());
//             }
//             Client{ws_out: ws_out, observers: to_use_observers,
//                 asset_pairs_vec: asset_pairs_vec.clone(),
//                 subscriptions_vec: subscriptions_vec.clone(),
//                 last_update: std::time::Instant::now()}
//         };
//     let settings = ws::Settings{
//         ..Default::default()
//     };
//     let mut websocket = ws::Builder::new().with_settings(settings).build(factory).unwrap();
//
//
//     websocket.connect(url::Url::parse("wss://ws.kraken.com").unwrap());
//     websocket.run();
// }

struct Client
{
    ws_out: ws::Sender,
    observers: Vec<Box<dyn Observer>>,
    asset_pairs_vec: std::vec::Vec<String>,
    subscriptions_vec: std::vec::Vec<String>,
    last_update: std::time::Instant
}


// impl io::Handler for Client
// {
//     fn random_function(&mut self)
//     {
//         self.run();
//
//     }
//
// }
// impl Client
// {
//     fn attempt_connect(&mut self)
//     {
//         println!("Attempting connect.");
//         let result= self.ws_out.connect(url::Url::parse("wss://ws.kraken.com").unwrap());
//
//
//         match result
//         {
//             Ok(T) => {println!("Got Ok on connect");},
//             Err(E) => {println!("Connection failed, retrying."); self.attempt_connect();}
//         }
//     }
// }

impl ws::Handler for Client
{
    fn on_open(&mut self, shake: ws::Handshake) -> ws::Result<()> {
        if let Some(_addr) = shake.remote_addr()? {
            // debug!("Connection with {} now open", addr);
            // for asset_pair in &self.asset_pairs_vec
            // {
            for subscription in &self.subscriptions_vec
            {
                if let Err(error) = self.ws_out.send(["{ \"event\": \"subscribe\",\
                      \"pair\": [\"", self.asset_pairs_vec.join("\", \"").as_str(), "\"],\
                      \"subscription\": ", subscription.as_str(), "}"
                ].join(""))
                {
                    println!("ERROR: {}", error);
                }
                println!("Sent: {}", ["{ \"event\": \"subscribe\",\
              \"pair\": [\"", self.asset_pairs_vec.join("\", \"").as_str(), "\"],\
              \"subscription\":", subscription.as_str()
                ].join(""));
                std::thread::sleep(std::time::Duration::from_millis(500)); //avoiding rate limiting
                // }
            }
        }
        Ok(())
    }
    fn on_timeout(&mut self, _event: ws::util::Token) -> ws::Result<()>
    {
        println!("Timed out");
        let mut to_transfer_observers: Vec<Box<dyn Observer>> = vec![];
        while self.observers.len() > 0
        {
           to_transfer_observers.push(self.observers.pop().unwrap());
        }
        connect(self.asset_pairs_vec.clone(), self.subscriptions_vec.clone(),
                    to_transfer_observers);
        // self.attempt_connect();
        Ok(())
    }

    fn on_error(&mut self, error: ws::Error)
    {
        println!("Error: {}", error);
        let mut to_transfer_observers: Vec<Box<dyn Observer>> = vec![];
        while self.observers.len() > 0
        {
           to_transfer_observers.push(self.observers.pop().unwrap());
        }
        connect(self.asset_pairs_vec.clone(), self.subscriptions_vec.clone(),
                    to_transfer_observers);

        // self.attempt_connect();

    }
    fn on_close(&mut self, code: ws::CloseCode, reason: &str)
    {
        let code_int: u16 = code.into();
        println!("Closing: {} {}", reason, code_int);
        let mut to_transfer_observers: Vec<Box<dyn Observer>> = vec![];
        while self.observers.len() > 0
        {
           to_transfer_observers.push(self.observers.pop().unwrap());
        }
        connect(self.asset_pairs_vec.clone(), self.subscriptions_vec.clone(),
                    to_transfer_observers);

        // self.attempt_connect()
        

    }

    fn on_message(&mut self, msg: ws::Message) -> ws::Result<()>
    {
        // let msg2: ws::Message = msg.clone();

        let message_data: std::string::String
            = msg.into_text().unwrap();
        // println!("{}", message_data);
        let message_json: json::JsonValue
            = json::parse(message_data.as_str()).unwrap();
        // println!("{}", message_json);

        for observer in self.observers.iter_mut()
        {
           observer.receive_message(&message_json);
        }
        // println!("{}", self.last_update.elapsed() > std::time::Duration::new(10, 0));
        if self.last_update.elapsed() > std::time::Duration::new(600, 0)
        {
            for subscription in &self.subscriptions_vec
            {
                if (subscription.len() > 20) && (subscription[0..26] == String::from("{\"name\": \"book\", \"depth\": "))
                {
                    if true
                    {

                        if let Err(error) = self.ws_out.send(["{ \"event\": \"subscribe\",\
                          \"pair\": [\"", self.asset_pairs_vec.join("\", \"").as_str(), "\"],\
                          \"subscription\": ", subscription.as_str(), "}"
                        ].join(""))
                        {
                            println!("ERROR: {}", error);
                        }
                        println!("Sent: {}", ["{ \"event\": \"subscribe\",\
                                  \"pair\": [\"", self.asset_pairs_vec.join("\", \"").as_str(), "\"],\
                                  \"subscription\":", subscription.as_str()
                                  ].join(""));
                        std::thread::sleep(std::time::Duration::from_millis(500)); //avoiding rate limiting
                        // }
                    }
                }
            }
            self.last_update = std::time::Instant::now();


        }
        // (self.observers[0])(msg2);
        // println!("Message: {}", json::stringify(message_json));

        // json::stringify(message_json);
        Ok(())
    }
}
