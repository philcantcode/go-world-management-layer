package worldv1

const (
	// DefaultMaxMessageSize is the default protobuf send and receive bound used
	// by both the public client and server.
	DefaultMaxMessageSize = 4 << 20

	ResetModeBaseline = "baseline"
	ResetModeRecreate = "recreate"
	ResetModeSnapshot = "snapshot"
)
