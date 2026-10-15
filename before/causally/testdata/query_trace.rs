use before::{Party, Version, Span};
use before::causally::*;
use std::io::Write;
fn emit<P:Polarity>(out:&mut Vec<u8>,q:Query<'_,P>,versions:&[Version],spans:&[Span<'_>]) {
 for v in versions { out.push(q.contains(v) as u8); }
 for s in spans { out.push(match q.coverage(s) { Coverage::Empty=>0, Coverage::Partial=>1, Coverage::Full=>2 }); }
}
fn main() {
 let mut p=Party::seed(); let q=p.fork(); let mut versions=Vec::new();
 for x in 0u64..3 { for y in 0u64..3 {
  let mut a=Version::new(); a.ticks(&p,x); let mut b=Version::new(); b.ticks(&q,y);
  versions.push(a.join(&b));
 } }
 let mut spans=Vec::new(); for lo in &versions { for hi in &versions { if let Ok(s)=Span::new(lo,hi) { spans.push(s); } } }
 let mut out=Vec::new();
 for a in &versions { for b in &versions {
  emit(&mut out,all(),&versions,&spans);
  emit(&mut out,Query::from(after(a)),&versions,&spans);
  emit(&mut out,Query::from(before(a)),&versions,&spans);
  emit(&mut out,strictly_after(a),&versions,&spans);
  emit(&mut out,strictly_before(a),&versions,&spans);
  emit(&mut out,since(a),&versions,&spans);
  emit(&mut out,until(a),&versions,&spans);
  emit(&mut out,after(a).or_concurrent(),&versions,&spans);
  emit(&mut out,before(a).or_concurrent(),&versions,&spans);
  emit(&mut out,delta(a,b),&versions,&spans);
  emit(&mut out,toward(a,b),&versions,&spans);
  emit(&mut out,after(a)&before(b),&versions,&spans);
  emit(&mut out,since(a)&since(b),&versions,&spans);
  emit(&mut out,until(a)&until(b),&versions,&spans);
 } }
 std::io::stdout().write_all(&out).unwrap();
}
