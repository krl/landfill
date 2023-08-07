use std::io;

use blake3::Hasher;
use landfill::{Content, Landfill};

const A_LOT: u64 = 1024;

#[test]
fn lots_of_content() -> Result<(), io::Error> {
    let lf = Landfill::ephemeral()?;
    let content: Content<Hasher> = lf.substructure("content")?;

    let mut ids = vec![];

    for i in 0u64..A_LOT {
        ids.push(content.insert(&i.to_le_bytes())?);
    }

    for (i, id) in ids.iter().enumerate() {
        assert_eq!(content.get(*id).unwrap(), i.to_le_bytes());
    }

    Ok(())
}
