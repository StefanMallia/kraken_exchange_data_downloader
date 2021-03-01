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
        println!("Connected to websocket.");
        // work around for transferring ownership of Observers from this closure
        // to Client
        let mut to_use_observers: Vec<Box<dyn Observer>> = vec![];
        while self.observers.len() > 0
        {
            to_use_observers.push(self.observers.pop().unwrap());
        }
        Client{ws_out: ws_out, observers: to_use_observers,
            asset_pairs_vec: self.asset_pairs_vec.clone(),
            subscriptions_vec: self.subscriptions_vec.clone(),
            last_update: std::time::Instant::now()}
    }
    fn connection_lost(&mut self, mut client: Client)
    {
        println!("Connection lost!");
    }
}

pub fn connect(asset_pairs_vec: std::vec::Vec<String>,
               subscriptions_vec: std::vec::Vec<String>,//,
               mut observers: Vec<Box<dyn Observer>>)
{

    let mut to_use_observers: Vec<Box<dyn Observer>> = vec![];
    while observers.len() > 0
    {
        to_use_observers.push(observers.pop().unwrap());
    }
    let factory = MyFactory{observers: to_use_observers,
        asset_pairs_vec: asset_pairs_vec.clone(),
        subscriptions_vec: subscriptions_vec.clone()};
    let settings = ws::Settings {
        ..Default::default()
    };
    let mut websocket = ws::Builder::new().with_settings(settings).build(factory).unwrap();
    websocket.connect(url::Url::parse("wss://ws.kraken.com").unwrap());
    websocket.run();
}

struct Client
{
    ws_out: ws::Sender,
    observers: Vec<Box<dyn Observer>>,
    asset_pairs_vec: std::vec::Vec<String>,
    subscriptions_vec: std::vec::Vec<String>,
    last_update: std::time::Instant
}

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
        //avoid too many attempts in short time
        std::thread::sleep(std::time::Duration::from_millis(1000));
        connect(self.asset_pairs_vec.clone(), self.subscriptions_vec.clone(),
                    to_transfer_observers);
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
        //avoid too many attempts in short time
        std::thread::sleep(std::time::Duration::from_millis(1000));
        connect(self.asset_pairs_vec.clone(), self.subscriptions_vec.clone(),
                    to_transfer_observers);
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
        //avoid too many attempts in short time
        std::thread::sleep(std::time::Duration::from_millis(1000));
        connect(self.asset_pairs_vec.clone(), self.subscriptions_vec.clone(),
                    to_transfer_observers);
    }

    fn on_message(&mut self, msg: ws::Message) -> ws::Result<()>
    {
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
            // self.ws_out.close(ws::CloseCode::Normal);
        }
        Ok(())
    }
}