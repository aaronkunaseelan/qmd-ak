package journal

import "time"

// EpochLayout is the native producer's UTC nanosecond journal identity.
const EpochLayout = "20060102T150405.000000000Z"

func FormatEpoch(t time.Time) string { return t.UTC().Format(EpochLayout) }
