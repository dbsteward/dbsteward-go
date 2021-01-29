package pgsql8

const MAX_IDENT_LENGTH = 63

const DataTypeSerial = "serial"
const DataTypeBigSerial = "bigserial"
const DataTypeInt = "int"
const DataTypeBigInt = "bigint"

const SequenceNameSuffix = "_seq"

// Version X.Y is encoded as X*1000+Y
const Version12_0 = 12000
