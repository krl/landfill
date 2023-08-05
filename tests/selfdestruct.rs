mod with_temp_path;
use with_temp_path::with_temp_path;

use landfill::{Landfill, RandomAccess};

#[test]
fn self_destruct() -> Result<(), std::io::Error> {
    with_temp_path(|path| {
        {
            {
                let lf = Landfill::open(path)?;

                let ao = RandomAccess::<u32>::try_from(&lf)?;

                for i in 1..=1024 {
                    ao.with_mut(i, |slot| *slot = i as u32)?;
                }
            }

            // re-open

            let lf = Landfill::open(path)?;
            let ao = RandomAccess::<u32>::try_from(&lf)?;

            for i in 1..=1024 {
                assert_eq!(*ao.get(i).unwrap(), i as u32)
            }

            // destroy
            lf.initiate_self_destruct_sequence()
        }

        // re-re-open

        let lf = Landfill::open(path)?;
        let ao = RandomAccess::<u32>::try_from(&lf)?;

        assert!(ao.get(1).is_none());

        Ok(())
    })
}
