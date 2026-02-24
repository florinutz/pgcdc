package walreplication

// oidToTypeName maps common PostgreSQL type OIDs to their type names.
// This avoids a round-trip to pg_type for the most common types.
// Unknown OIDs fall back to "oid:<N>".
var oidToTypeName = map[uint32]string{
	16:   "bool",
	17:   "bytea",
	18:   "char",
	19:   "name",
	20:   "int8",
	21:   "int2",
	23:   "int4",
	24:   "regproc",
	25:   "text",
	26:   "oid",
	114:  "json",
	142:  "xml",
	600:  "point",
	650:  "cidr",
	700:  "float4",
	701:  "float8",
	718:  "circle",
	790:  "money",
	829:  "macaddr",
	869:  "inet",
	1042: "bpchar",
	1043: "varchar",
	1082: "date",
	1083: "time",
	1114: "timestamp",
	1184: "timestamptz",
	1186: "interval",
	1266: "timetz",
	1560: "bit",
	1562: "varbit",
	1700: "numeric",
	2950: "uuid",
	3614: "tsvector",
	3615: "tsquery",
	3802: "jsonb",
	3904: "int4range",
	3906: "numrange",
	3908: "tsrange",
	3910: "tstzrange",
	3912: "daterange",
	3926: "int8range",
}

// TypeNameForOID returns the PostgreSQL type name for the given OID.
// Returns "oid:<N>" for unknown types.
func TypeNameForOID(oid uint32) string {
	if name, ok := oidToTypeName[oid]; ok {
		return name
	}
	return "oid:" + uitoa(oid)
}

// uitoa converts uint32 to string without importing strconv.
func uitoa(n uint32) string {
	if n == 0 {
		return "0"
	}
	buf := [10]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// ColumnSchema represents the type metadata for a single column.
type ColumnSchema struct {
	Name     string `json:"name"`
	TypeOID  uint32 `json:"type_oid"`
	TypeName string `json:"type_name"`
}
