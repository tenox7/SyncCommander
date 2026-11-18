//go:build !linux && !darwin && !freebsd

package transport

import "os"

// makeTempHome makes no home on a system with no lock that tells a live
// process's home from an ended one's: temp folders go straight in the
// temp folder, as before.
func makeTempHome(string) (string, *os.File, error) { return "", nil, nil }

// sweepTempHomes sweeps nothing: there are no homes.
func sweepTempHomes(string) {}
