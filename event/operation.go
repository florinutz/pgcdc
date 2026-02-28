package event

// Operation represents the type of database change in a structured record.
type Operation int

const (
	OperationCreate   Operation = iota + 1 // INSERT
	OperationUpdate                        // UPDATE
	OperationDelete                        // DELETE
	OperationTruncate                      // TRUNCATE
	OperationSnapshot                      // SNAPSHOT (initial data load)
)

// String returns the legacy operation name used in JSON payloads and the
// Event.Operation field, ensuring backward compatibility.
func (op Operation) String() string {
	switch op {
	case OperationCreate:
		return "INSERT"
	case OperationUpdate:
		return "UPDATE"
	case OperationDelete:
		return "DELETE"
	case OperationTruncate:
		return "TRUNCATE"
	case OperationSnapshot:
		return OpSnapshot
	default:
		return ""
	}
}

// ParseOperation converts a string operation name to an Operation value.
// Returns 0 for unrecognized operations (e.g., NOTIFY, BEGIN, COMMIT,
// SCHEMA_CHANGE).
func ParseOperation(s string) Operation {
	switch s {
	case "INSERT":
		return OperationCreate
	case "UPDATE":
		return OperationUpdate
	case "DELETE":
		return OperationDelete
	case "TRUNCATE":
		return OperationTruncate
	case OpSnapshot:
		return OperationSnapshot
	default:
		return 0
	}
}
