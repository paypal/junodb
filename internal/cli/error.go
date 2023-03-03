package cli

import ()

type IRetryable interface {
	Retryable() bool
}

type Error struct {
	What string
}

func (e *Error) Retryable() bool { return false }

type RetryableError struct {
	What string
}

func (e *RetryableError) Retryable() bool { return true }

func (e *Error) Error() string {
	return "error: " + e.What
}

func (e *RetryableError) Error() string {
	return "error: " + e.What
}

func NewError(err error) *Error {
	return &Error{
		What: err.Error(),
	}
}

func NewErrorWithString(err string) *Error {
	return &Error{err}
}

/*
	// DON'T RETRY,
	//   non-system error, invocation method can handle this response codes
*	case OperationStatus::NoKey:
*	case OperationStatus::DupKey:
	case OperationStatus::DataExpired:
*	case OperationStatus::BadParam:
	case OperationStatus::VersionTooOld:
	case OperationStatus::VersionConflict:
*	case OperationStatus::NoUncommitted:
	case OperationStatus::DuplicateRequest:
	case OperationStatus::NotAppendable:
		return true;

		// RETRY following response codes
*	case OperationStatus::BadMsg:
*	case OperationStatus::OutOfMem:
*	case OperationStatus::NoStorageServer:
	case OperationStatus::StorageServerTimeout:
*	case OperationStatus::RecordLocked:
*	case OperationStatus::BadRequestID:
		// check rethrow flag to figure out if
		// we have to throw exception or return false
		if (_rethrow)
		{
			_trans.AddData(CLIENT_CAL_EXCEPTION, CLIENT_CAL_BADPARAM_EXCEPTION);
			m_tmp_string.copy_formatted("MayflyServer:RetriableSystemError responseStatus='%s'. Response='%s' ",
					OperationStatus::get_err_text(responseStatus),
					_response.to_string().chars());
			_trans.AddData(CLIENT_CAL_DETAILS, m_tmp_string);
			WRITE_LOG_ENTRY(&m_logger, LOG_WARNING, m_tmp_string.chars());

			_trans.SetStatus(CalTransaction::Status(CAL::TRANS_ERROR, CLIENT_CAL_TYPE, CAL::SYS_ERR_INTERNAL, "-1"));
			throw BadParamException(m_tmp_string);
		}
		return false;

		// DONT'T RETRY, system error throw exception
*	case OperationStatus::ServiceDenied:
	case OperationStatus::Inserting:
*/
