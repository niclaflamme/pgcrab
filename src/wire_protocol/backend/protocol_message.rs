use super::frames::authentication_cleartext_password::AuthenticationCleartextPasswordFrame;
use super::frames::authentication_gss::AuthenticationGssFrame;
use super::frames::authentication_gss_continue::AuthenticationGssContinueFrame;
use super::frames::authentication_kerberos_v5::AuthenticationKerberosV5Frame;
use super::frames::authentication_md5_password::AuthenticationMd5PasswordFrame;
use super::frames::authentication_ok::AuthenticationOkFrame;
use super::frames::authentication_sasl::AuthenticationSaslFrame;
use super::frames::authentication_sasl_continue::AuthenticationSaslContinueFrame;
use super::frames::authentication_sasl_final::AuthenticationSaslFinalFrame;
use super::frames::authentication_scm_credential::AuthenticationScmCredentialFrame;
use super::frames::authentication_sspi::AuthenticationSspiFrame;
use super::frames::backend_key_data::BackendKeyDataFrame;
use super::frames::bind_complete::BindCompleteFrame;
use super::frames::close_complete::CloseCompleteFrame;
use super::frames::command_complete::CommandCompleteFrame;
use super::frames::copy_both_response::CopyBothResponseFrame;
use super::frames::copy_data::CopyDataFrame;
use super::frames::copy_done::CopyDoneFrame;
use super::frames::copy_in_response::CopyInResponseFrame;
use super::frames::copy_out_response::CopyOutResponseFrame;
use super::frames::data_row::DataRowFrame;
use super::frames::empty_query_response::EmptyQueryResponseFrame;
use super::frames::error_response::ErrorResponseFrame;
use super::frames::function_call_response::FunctionCallResponseFrame;
use super::frames::negotiate_protocol_version::NegotiateProtocolVersionFrame;
use super::frames::no_data::NoDataFrame;
use super::frames::notice_response::NoticeResponseFrame;
use super::frames::notification_response::NotificationResponseFrame;
use super::frames::parameter_description::ParameterDescriptionFrame;
use super::frames::parameter_status::ParameterStatusFrame;
use super::frames::parse_complete::ParseCompleteFrame;
use super::frames::portal_suspended::PortalSuspendedFrame;
use super::frames::ready_for_query::ReadyForQueryFrame;
use super::frames::row_description::RowDescriptionFrame;

/// Represents any backend-initiated protocol message.
/// Bidirectional protocol messages are also included.
#[derive(Debug)]
pub enum BackendProtocolMessage<'a> {
    /// AuthenticationCleartextPassword message
    AuthenticationCleartextPassword(AuthenticationCleartextPasswordFrame),

    /// AuthenticationGss message
    AuthenticationGss(AuthenticationGssFrame),

    /// AuthenticationGssContinue message
    AuthenticationGssContinue(AuthenticationGssContinueFrame<'a>),

    /// AuthenticationKerberosV5 message
    AuthenticationKerberosV5(AuthenticationKerberosV5Frame),

    /// AuthenticationMd5Password message
    AuthenticationMd5Password(AuthenticationMd5PasswordFrame),

    /// AuthenticationOk message
    AuthenticationOk(AuthenticationOkFrame),

    /// AuthenticationSasl message
    AuthenticationSasl(AuthenticationSaslFrame),

    /// AuthenticationSaslContinue message
    AuthenticationSaslContinue(AuthenticationSaslContinueFrame<'a>),

    /// AuthenticationSaslFinal message
    AuthenticationSaslFinal(AuthenticationSaslFinalFrame<'a>),

    /// AuthenticationScmCredential message
    AuthenticationScmCredential(AuthenticationScmCredentialFrame),

    /// AuthenticationSspi message
    AuthenticationSspi(AuthenticationSspiFrame),

    /// BackendKeyData message
    BackendKeyData(BackendKeyDataFrame),

    /// BindComplete message
    BindComplete(BindCompleteFrame),

    /// CloseComplete message
    CloseComplete(CloseCompleteFrame),

    /// CommandComplete message
    CommandComplete(CommandCompleteFrame<'a>),

    /// CopyBothResponse message
    CopyBothResponse(CopyBothResponseFrame),

    /// CopyData message for COPY operations
    CopyData(CopyDataFrame<'a>),

    /// CopyDone message for COPY operations
    CopyDone(CopyDoneFrame),

    /// CopyInResponse message
    CopyInResponse(CopyInResponseFrame),

    /// CopyOutResponse message
    CopyOutResponse(CopyOutResponseFrame),

    /// DataRow message
    DataRow(DataRowFrame<'a>),

    /// EmptyQueryResponse message
    EmptyQueryResponse(EmptyQueryResponseFrame),

    /// ErrorResponse message
    ErrorResponse(ErrorResponseFrame<'a>),

    /// FunctionCallResponse message
    FunctionCallResponse(FunctionCallResponseFrame<'a>),

    /// NegotiateProtocolVersion message
    NegotiateProtocolVersion(NegotiateProtocolVersionFrame<'a>),

    /// NoData message
    NoData(NoDataFrame),

    /// NoticeResponse message
    NoticeResponse(NoticeResponseFrame<'a>),

    /// NotificationResponse message
    NotificationResponse(NotificationResponseFrame<'a>),

    /// ParameterDescription message
    ParameterDescription(ParameterDescriptionFrame),

    /// ParameterStatus message
    ParameterStatus(ParameterStatusFrame<'a>),

    /// ParseComplete message
    ParseComplete(ParseCompleteFrame),

    /// PortalSuspended message
    PortalSuspended(PortalSuspendedFrame),

    /// ReadyForQuery message
    ReadyForQuery(ReadyForQueryFrame),

    /// RowDescription message
    RowDescription(RowDescriptionFrame<'a>),
}
