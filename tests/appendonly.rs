use landfill::{AppendOnly, Landfill};

mod with_temp_path;
use with_temp_path::with_temp_path;

#[test]
fn appendonly_trivial() -> Result<(), std::io::Error> {
    let lf = Landfill::ephemeral()?;
    let ao: AppendOnly = lf.substructure("ao")?;

    let msg_a = b"hello word";
    let msg_b = b"hello world!";

    let ofs_a = ao.write(msg_a)?;

    let slice_a = ao.get_slice::<u8>(ofs_a, msg_a.len());

    let ofs_b = ao.write(msg_b)?;

    let slice_b = ao.get_slice::<u8>(ofs_b, msg_b.len());

    assert_eq!(slice_a, msg_a);
    assert_eq!(slice_b, msg_b);

    Ok(())
}

#[test]
fn appendonly_save_restore() -> Result<(), std::io::Error> {
    with_temp_path(|path| {
        let ofs_a;
        let ofs_b;

        let msg_a = b"hello word";
        let msg_b = b"hello world!";

        {
            let lf = Landfill::open(path)?;
            let ao: AppendOnly = lf.substructure("ao")?;

            ofs_a = ao.write(msg_a)?;

            let slice_a = ao.get_slice::<u8>(ofs_a, msg_a.len());

            ofs_b = ao.write(msg_b)?;

            let slice_b = ao.get_slice::<u8>(ofs_b, msg_b.len());

            assert_eq!(slice_a, msg_a);
            assert_eq!(slice_b, msg_b);
        }

        let lf = Landfill::open(path)?;

        // re-open

        let ao: AppendOnly = lf.substructure("ao")?;
        assert_eq!(ao.get_slice::<u8>(ofs_a, msg_a.len()), msg_a);
        assert_eq!(ao.get_slice::<u8>(ofs_b, msg_b.len()), msg_b);

        Ok(())
    })
}

#[test]
fn appendonly_larger_type() -> Result<(), std::io::Error> {
    with_temp_path(|path| {
        let ofs_a;
        let ofs_b;

        let msg_a = &[7u32, 1, 0, 0, 1999, 8];
        let msg_b = &[3u32, 4];

        {
            let lf = Landfill::open(path)?;
            let ao: AppendOnly = lf.substructure("ao")?;

            ofs_a = ao.write(msg_a)?;

            let slice_a = ao.get_slice::<u32>(ofs_a, msg_a.len());

            ofs_b = ao.write(msg_b)?;

            let slice_b = ao.get_slice::<u32>(ofs_b, msg_b.len());

            assert_eq!(slice_a, msg_a);
            assert_eq!(slice_b, msg_b);
        }

        let lf = Landfill::open(path)?;

        // re-open

        let ao: AppendOnly = lf.substructure("ao")?;
        assert_eq!(ao.get_slice::<u32>(ofs_a, msg_a.len()), msg_a);
        assert_eq!(ao.get_slice::<u32>(ofs_b, msg_b.len()), msg_b);

        Ok(())
    })
}

#[test]
fn appendonly_save_restore_skip_files() -> Result<(), std::io::Error> {
    with_temp_path(|path| {
        let ofs_a;
        let ofs_b;

        let msg_a = b"hello word";
        let msg_b = b"hello world!";

        {
            let lf = Landfill::open(path)?;
            let ao: AppendOnly = lf.substructure("ao")?;

            ofs_a = ao.write(msg_a)?;

            let slice_a = ao.get_slice::<u8>(ofs_a, msg_a.len());

            ofs_b = ao.write(msg_b)?;

            let slice_b = ao.get_slice::<u8>(ofs_b, msg_b.len());

            assert_eq!(slice_a, msg_a);
            assert_eq!(slice_b, msg_b);
        }

        // re-open

        let lf = Landfill::open(path)?;
        let ao: AppendOnly = lf.substructure("ao")?;

        assert_eq!(ao.get_slice::<u8>(ofs_a, msg_a.len()), msg_a);
        assert_eq!(ao.get_slice::<u8>(ofs_b, msg_b.len()), msg_b);

        Ok(())
    })
}
