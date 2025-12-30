use bytes::{BufMut, Bytes, BytesMut};

// -----------------------------------------------------------------------------
// ----- ErrorResponse ---------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct ErrorResponse {
    pub severity: Severity, // S
    pub code: &'static str, // C (SQLSTATE 5-char)
    pub message: String,    // M

    pub detail: Option<String>,          // D
    pub hint: Option<String>,            // H
    pub position: Option<u32>,           // P
    pub internal_position: Option<u32>,  // p
    pub internal_query: Option<String>,  // q
    pub where_: Option<String>,          // W
    pub schema_name: Option<String>,     // s
    pub table_name: Option<String>,      // t
    pub column_name: Option<String>,     // c
    pub data_type_name: Option<String>,  // d
    pub constraint_name: Option<String>, // n
    pub file: Option<String>,            // F
    pub line: Option<u32>,               // L
    pub routine: Option<String>,         // R
}

// -----------------------------------------------------------------------------
// ----- ErrorResponse: Static -------------------------------------------------

impl ErrorResponse {
    pub fn new(severity: Severity, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            severity,
            code,
            message: message.into(),
            ..Default::default()
        }
    }

    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, "XX000", message)
    }

    pub fn protocol_violation(message: impl Into<String>) -> Self {
        Self::new(Severity::Fatal, "08P01", message)
    }

    pub fn invalid_password(message: impl Into<String>) -> Self {
        Self::new(Severity::Fatal, "28P01", message)
    }
}

// -----------------------------------------------------------------------------
// ----- ErrorResponse: Builder ------------------------------------------------

impl ErrorResponse {
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_where(mut self, where_: impl Into<String>) -> Self {
        self.where_ = Some(where_.into());
        self
    }

    pub fn with_file(mut self, file: impl Into<String>) -> Self {
        self.file = Some(file.into());
        self
    }

    pub fn with_routine(mut self, routine: impl Into<String>) -> Self {
        self.routine = Some(routine.into());
        self
    }

    pub fn with_position(mut self, pos: u32) -> Self {
        self.position = Some(pos);
        self
    }

    pub fn with_internal_position(mut self, pos: u32) -> Self {
        self.internal_position = Some(pos);
        self
    }

    pub fn with_internal_query(mut self, q: impl Into<String>) -> Self {
        self.internal_query = Some(q.into());
        self
    }

    pub fn with_schema(mut self, v: impl Into<String>) -> Self {
        self.schema_name = Some(v.into());
        self
    }

    pub fn with_table(mut self, v: impl Into<String>) -> Self {
        self.table_name = Some(v.into());
        self
    }

    pub fn with_column(mut self, v: impl Into<String>) -> Self {
        self.column_name = Some(v.into());
        self
    }

    pub fn with_data_type(mut self, v: impl Into<String>) -> Self {
        self.data_type_name = Some(v.into());
        self
    }

    pub fn with_constraint(mut self, v: impl Into<String>) -> Self {
        self.constraint_name = Some(v.into());
        self
    }

    pub fn with_line(mut self, v: u32) -> Self {
        self.line = Some(v);
        self
    }
}

// -----------------------------------------------------------------------------
// ----- ErrorResponse: Public -------------------------------------------------

impl ErrorResponse {
    /// Build the backend 'E' frame. Returns a complete wire buffer.
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(256);

        buf.put_u8(b'E'); // message type
        buf.put_u32(0); // length placeholder

        put_field(&mut buf, b'S', self.severity.as_str());
        // Optional: also include nonlocalized severity ('V') if you want
        // put_field(&mut buf, b'V', self.severity.as_str());

        put_field(&mut buf, b'C', self.code);
        put_field(&mut buf, b'M', &self.message);

        if let Some(v) = self.detail.as_deref() {
            put_field(&mut buf, b'D', v);
        }

        if let Some(v) = self.hint.as_deref() {
            put_field(&mut buf, b'H', v);
        }

        if let Some(v) = self.position {
            let position_string = itoa(v);
            put_field(&mut buf, b'P', &position_string);
        }

        if let Some(v) = self.internal_position {
            let internal_position_string = itoa(v);
            put_field(&mut buf, b'p', &internal_position_string);
        }

        if let Some(v) = self.internal_query.as_deref() {
            put_field(&mut buf, b'q', v);
        }

        if let Some(v) = self.where_.as_deref() {
            put_field(&mut buf, b'W', v);
        }

        if let Some(v) = self.schema_name.as_deref() {
            put_field(&mut buf, b's', v);
        }

        if let Some(v) = self.table_name.as_deref() {
            put_field(&mut buf, b't', v);
        }

        if let Some(v) = self.column_name.as_deref() {
            put_field(&mut buf, b'c', v);
        }

        if let Some(v) = self.data_type_name.as_deref() {
            put_field(&mut buf, b'd', v);
        }

        if let Some(v) = self.constraint_name.as_deref() {
            put_field(&mut buf, b'n', v);
        }

        if let Some(v) = self.file.as_deref() {
            put_field(&mut buf, b'F', v);
        }

        if let Some(v) = self.line {
            let line_string = itoa(v);
            put_field(&mut buf, b'L', &line_string);
        }

        if let Some(v) = self.routine.as_deref() {
            put_field(&mut buf, b'R', v);
        }

        buf.put_u8(0); // terminator

        // In to_bytes, replace the length backfill with:
        let len = (buf.len() - 1) as u32;
        buf[1..5].copy_from_slice(&len.to_be_bytes());

        buf.freeze()
    }
}

// -----------------------------------------------------------------------------
// ----- ErrorResponse: Severity -----------------------------------------------

#[derive(Clone, Copy, Debug)]
pub enum Severity {
    Error,
    Fatal,
    Panic,
    Warning,
    Notice,
    Info,
    Debug,
    Log,
}

impl Severity {
    fn as_str(self) -> &'static str {
        match self {
            Severity::Error => "ERROR",
            Severity::Fatal => "FATAL",
            Severity::Panic => "PANIC",
            Severity::Warning => "WARNING",
            Severity::Notice => "NOTICE",
            Severity::Info => "INFO",
            Severity::Debug => "DEBUG",
            Severity::Log => "LOG",
        }
    }
}

impl Default for Severity {
    fn default() -> Self {
        Severity::Error
    }
}

// -----------------------------------------------------------------------------
// ----- Internal: Helpers -----------------------------------------------------

#[inline]
fn put_field(buf: &mut BytesMut, tag: u8, val: &str) {
    buf.put_u8(tag);
    buf.extend_from_slice(val.as_bytes());
    buf.put_u8(0);
}

#[inline]
fn itoa(v: u32) -> String {
    // No external deps; cheap enough.
    v.to_string()
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_minimal_error() {
        let e = ErrorResponse::new(Severity::Error, "XX000", "boom");
        let b = e.to_bytes();
        assert_eq!(b[0], b'E');
        assert!(b.len() > 12);
    }

    #[test]
    fn includes_optional_fields() {
        let e = ErrorResponse::new(Severity::Fatal, "08P01", "bad protocol")
            .with_detail("expected StartupMessage")
            .with_hint("check client SSL negotiation")
            .with_position(42)
            .with_schema("public")
            .with_table("users")
            .with_column("username")
            .with_file("backend.c")
            .with_line(1337)
            .with_routine("serve_startup");
        let b = e.to_bytes();
        assert!(b.windows(7).any(|w| w == b"SFATAL\0")); // crude sanity
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
