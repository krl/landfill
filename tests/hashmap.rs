use std::io;

use landfill::Landfill;
use landfill::{HashMap, Search};

#[test]
fn trivial() -> Result<(), io::Error> {
    let lf = Landfill::ephemeral()?;

    let h = HashMap::try_from(lf)?;

    h.find_space_for(b"omg", |_| Search::Continue, || 3)?;

    Ok(())
}
