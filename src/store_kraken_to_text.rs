use crate::message_types::{KrakenMessage};
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;



pub struct TextFile
{
    pub output_directory: String,
}
impl TextFile
{
    pub fn new(output_directory: &str) -> TextFile
    {
        let output_directory: &str = output_directory;
        TextFile{output_directory: output_directory.to_string()}
    }

    pub fn append_to_file(&mut self, file_name: &str,
                         kraken_messages: &Vec<Box<json::JsonValue>>)
    {

        let mut file: File
            = OpenOptions::new().create(true).append(true).open([self.output_directory.to_string(),
                                                                   "/".to_string(),
                                                                   file_name.to_string()].concat()
                                                             ).expect("Can't open file.");

        for kraken_message in kraken_messages
        {
            file.write_all([kraken_message.to_string(),
                            "\n".to_string()
                           ].concat().as_bytes()).expect("write failed");
        }

    }

}
