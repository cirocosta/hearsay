// generate with the pinned rumors crate and its before path dependency.
use before::{Clock, Version};
use rumors::{EncodeError, Peer};

fn show(name: &str, replica: &rumors::Rumors<u64>) {
    let snapshot = replica.snapshot();
    let latest: String = snapshot.latest().as_bytes().iter().map(|b| format!("{b:02x}")).collect();
    let mut versions: Vec<String> = snapshot.versions().map(|v| v.as_bytes().iter().map(|b| format!("{b:02x}")).collect()).collect();
    versions.sort();
    println!("{name} {latest} {} {}", snapshot.len(), if versions.is_empty() { "-".to_owned() } else { versions.join(",") });
}

fn main() {
    let replica = Peer::<u64>::seed().into_rumors();
    let v = replica.send(7).unwrap();
    replica.redact_all([&v, &v]);
    show("duplicate-redact", &replica);

    let replica = Peer::<u64>::seed().into_rumors();
    let v = replica.send(7).unwrap();
    replica.redact_all([&v, &Version::new(), &v]);
    show("duplicate-around-missing", &replica);

    let mut clock = Clock::seed();
    clock.tick();
    let fresh = clock.version().clone();
    let replica = Peer::<u64>::seed().into_rumors();
    replica.batch(|batch| {
        batch.send(1)?;
        batch.redact(&fresh);
        Ok::<_, EncodeError>(())
    }).unwrap();
    show("cancel-fresh", &replica);
    replica.send(2).unwrap();
    show("send-after-cancel", &replica);

    let replica = Peer::<u64>::seed().into_rumors();
    replica.batch(|batch| {
        batch.send(1)?;
        batch.redact(&fresh);
        batch.send(2)?;
        Ok::<_, EncodeError>(())
    }).unwrap();
    show("cancel-then-send", &replica);

    let replica = Peer::<u64>::seed().into_rumors();
    let v = replica.send(7).unwrap();
    replica.batch(|batch| {
        batch.redact(&v);
        batch.redact(&v);
        batch.send(9)?;
        batch.redact(&v);
        Ok::<_, EncodeError>(())
    }).unwrap();
    show("mixed-final-key-stamps", &replica);

    let replica = Peer::<u64>::seed().into_rumors();
    replica.redact_all([&Version::new(), &Version::new()]);
    show("missing-only", &replica);
}
