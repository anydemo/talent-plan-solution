use serde::ser::{Serializer, SerializeSeq, SerializeMap};

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
enum Command {
    Ping(Option<String>),
    Set(String, String),
    Get(String),
    Remove(String)
}

impl Serialize for Command {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 3 is the number of fields in the struct.
        let msg = match self{
            Command::Ping(msg) => {
                match msg {
                    Some(m) => format!("*{}\r\n{}\r\n", m.len(), m),
                    None => "PING\r\n".to_owned(),
                }
            },
            _ => "unknown command".to_owned(),
        };

        serializer.serialize_str(&msg)
    }
}

#[cfg(test)]
mod test_resp {
    use super::*;
    use serde_test::{Token, assert_tokens, assert_ser_tokens};

    #[test]
    fn test_custom_serialize() {
        let cmd = Command::Ping(None);
        assert_ser_tokens(&cmd, &[
            Token::String("PING\r\n"),
        ]);
    }
}
