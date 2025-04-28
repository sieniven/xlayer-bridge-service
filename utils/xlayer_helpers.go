package utils

// GenerateTraceID generates a random trace ID.
func GenerateTraceID() string {
	return generateRandomString(traceIDLen)
}
