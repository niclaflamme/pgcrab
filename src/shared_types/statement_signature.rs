use md5::Context;

// -----------------------------------------------------------------------------
// ----- StatementSignature ----------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct StatementSignature(pub(crate) [u8; 16]);

impl StatementSignature {
    pub fn new(sql: &str, param_type_oids: &[i32]) -> Self {
        let mut ctx = Context::new();
        ctx.consume(sql.as_bytes());
        ctx.consume([0]);
        for oid in param_type_oids {
            ctx.consume(oid.to_be_bytes());
        }
        StatementSignature(ctx.compute().0)
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
