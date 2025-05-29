package estimatetime

type Config struct {
	// Time limits set for each L1, L2.
	// The average time calculated cannot exceed this limit.
	defaultTime []uint32

	// Number of DB records to calculate average time from.
	sampleLimit uint32
}
