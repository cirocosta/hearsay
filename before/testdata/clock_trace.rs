use before::Clock;
fn hex(c: &Clock) -> String { c.encode().iter().map(|b| format!("{b:02x}")).collect() }
fn main() {
 let mut clocks=vec![Clock::seed()];
 let mut state=0x179ea2b4cd712abdu64;
 for _ in 0..300 {
  state=state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
  let i=(state>>17) as usize % clocks.len();
  match state>>60 {
   0..=3 if clocks.len()<16 => {
    let c=clocks[i].fork(); clocks.push(c);
    println!("fork {i} {} {}",hex(&clocks[i]),hex(clocks.last().unwrap()));
   }
   4..=8 => { clocks[i].tick(); println!("tick {i} {}",hex(&clocks[i])); }
   9..=12 => {
    let j=(state>>35) as usize % clocks.len();
    let v=clocks[j].version().clone(); clocks[i].absorb(&v);
    println!("absorb {i} {j} {}",hex(&clocks[i]));
   }
   _ if clocks.len()>1 => {
    let c=clocks.pop().unwrap(); let i=i%clocks.len();
    clocks[i].join(c).unwrap(); println!("join {i} {}",hex(&clocks[i]));
   }
   _ => { clocks[i].tick(); println!("tick {i} {}",hex(&clocks[i])); }
  }
 }
}
