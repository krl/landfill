use landfill::AppendOnly;

mod with_temp_path;
use with_temp_path::with_temp_path;

#[test]
fn appendonly_trivial() -> Result<(), std::io::Error> {
    let ao = AppendOnly::<1024>::ephemeral()?;

    let msg_a = b"hello word";
    let msg_b = b"hello world!";

    let rec_a = ao.write(msg_a)?;

    let slice_a = ao.get(rec_a);

    let rec_b = ao.write(msg_b)?;

    let slice_b = ao.get(rec_b);

    assert_eq!(slice_a, msg_a);
    assert_eq!(slice_b, msg_b);

    Ok(())
}

#[test]
fn appendonly_save_restore() -> Result<(), std::io::Error> {
    with_temp_path(|path| {
        let rec_a;
        let rec_b;

        let msg_a = b"hello word";
        let msg_b = b"hello world!";

        {
            let ao = AppendOnly::<1024>::open(path)?;

            rec_a = ao.write(msg_a)?;

            let slice_a = ao.get(rec_a);

            rec_b = ao.write(msg_b)?;

            let slice_b = ao.get(rec_b);

            assert_eq!(slice_a, msg_a);
            assert_eq!(slice_b, msg_b);
        }

        // re-open

        let ao = AppendOnly::<1024>::open(path)?;
        assert_eq!(ao.get(rec_a), msg_a);
        assert_eq!(ao.get(rec_b), msg_b);

        Ok(())
    })
}

#[test]
fn appendonly_save_restore_skip_files() -> Result<(), std::io::Error> {
    with_temp_path(|path| {
        let rec_a;
        let rec_b;

        let msg_a = b"hello word";
        let msg_b = b"hello world!";

        {
            let ao = AppendOnly::<1>::open(path)?;

            rec_a = ao.write(msg_a)?;

            let slice_a = ao.get(rec_a);

            rec_b = ao.write(msg_b)?;

            let slice_b = ao.get(rec_b);

            assert_eq!(slice_a, msg_a);
            assert_eq!(slice_b, msg_b);
        }

        // re-open

        let ao = AppendOnly::<1>::open(path)?;
        assert_eq!(ao.get(rec_a), msg_a);
        assert_eq!(ao.get(rec_b), msg_b);

        Ok(())
    })
}
