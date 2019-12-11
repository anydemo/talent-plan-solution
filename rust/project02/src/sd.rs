use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Person {
    name: String,
    age: u8,
    phones: Vec<String>,
}

#[cfg(test)]
mod test_serde {
    use super::*;
    use serde_json::Deserializer;
    use std::fs::File;
    use std::io::prelude::*;
    use std::io::{BufReader, BufWriter};

    #[test]
    fn test_ser() {
        let data = r#"
        {
            "name": "John Doe",
            "age": 43,
            "phones": [
                "+44 1234567",
                "+44 2345678"
            ]
        }"#;

        let p: Person = serde_json::from_str(data).unwrap();
        println!("p = {:?}", p);
        let j = serde_json::to_string(&p).unwrap();
        let sp: Person = serde_json::from_str(&j).unwrap();
        assert_eq!(p, sp);
    }

    #[test]
    fn test_serde_file() {
        let mut p = Person {
            name: "p1".to_owned(),
            age: 10,
            phones: vec!["+44 1234567".to_owned(), "+44 1234568".to_owned()],
        };
        let f = File::create("data/foo.txt").unwrap();
        let writer = BufWriter::new(&f);
        serde_json::to_writer(writer, &p).unwrap();

        p.name = "p2".to_owned();
        let writer = BufWriter::new(&f);
        serde_json::to_writer(writer, &p).unwrap();

        p.name = "p3".to_owned();
        let writer = BufWriter::new(&f);
        serde_json::to_writer(writer, &p).unwrap();

        let ff = File::open("data/foo.txt").unwrap();
        let reader = BufReader::new(ff);
        let mut stream = Deserializer::from_reader(reader).into_iter::<Person>();
        while let Some(p) = stream.next() {
            println!("p = {:?}", p);
        }
    }
}
