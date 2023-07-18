use diskjockey::AppendOnly;

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
