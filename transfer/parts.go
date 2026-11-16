package transfer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"

	"sc/model"
	"sc/transport"
)

// partWriter returns dst as a model.PartWriter when it writes copies into
// part files, nil when it does not. An error when that cannot be told, as
// when a remote cannot be reached: the copy must not guess, or a copy that
// should go through a part would write the real file directly.
func partWriter(ctx context.Context, dst model.Backend) (model.PartWriter, error) {
	caps, err := dst.Capabilities(ctx)
	if err != nil || !caps.Parts {
		return nil, err
	}
	pw, _ := dst.(model.PartWriter)
	return pw, nil
}

// copyIntoPart copies f into its part file and renames the part over the
// real file once every byte is there and checked (review L14). The real file
// never holds half a copy, and an overwrite cut off half way leaves the old
// version in place. A part that a cut-off copy left is resumed only when its
// bytes are the source's first bytes by checksum on both sides: never on its
// size alone, and never on dates (PROTOCOLS.md, "Append or overwrite").
//
// The checksums run outside the stall guard, which watches bytes move: a
// checksum of many gigabytes moves none (F5). Reports whether it resumed.
func (c *copier) copyIntoPart(ctx context.Context, f item, pw model.PartWriter, slotBytes, slotBase *atomic.Int64) (resumed bool, err error) {
	src := f.src
	part := model.PartPath(f.relPath)
	attempt := 0
	// ctx from here on says this Retry owns the tries: a backend call under
	// it runs once, and a failed one fails this try (D4).
	err = transport.Retry(ctx, "copy", "copy "+f.relPath, func(ctx context.Context) error {
		attempt++
		bytes0, base0 := slotBytes.Load(), slotBase.Load()
		var offset int64
		var err error
		// A part can only be there when the scan listed one or an earlier
		// try left one: otherwise no stat, which costs a round trip.
		if f.part || attempt > 1 {
			offset, err = c.partOffset(ctx, pw, f.relPath, part, src.Size)
		}
		if err == nil {
			err = c.fillPart(ctx, f.relPath, part, src, &offset, slotBytes, slotBase)
		}
		if err == nil && offset > 0 && offset < src.Size && c.req.VerifyResume {
			err = c.verifyPart(ctx, f.relPath, part)
		}
		if err == nil {
			// The times go on the part, so the file has them from the moment
			// it has its name.
			if terr := c.dst.SetTimes(ctx, part, src.ModTime, src.ATime, src.BirthTime); terr != nil {
				transport.Log.Add("copy", transport.DirErr, "settimes "+f.relPath+": "+terr.Error())
			}
			err = pw.CommitPart(ctx, f.relPath, src.Mode)
		}
		if err != nil {
			// What this try credited is undone; the next one counts again.
			slotBytes.Store(bytes0)
			slotBase.Store(base0)
			return err
		}
		resumed = offset > 0
		return nil
	})
	return resumed, err
}

// partOffset returns where the copy can go on in relPath's part: the part's
// size when those bytes are the source's first bytes, else 0, from the
// start. Something other than a file at the part's name is removed: the
// name is sc's.
func (c *copier) partOffset(ctx context.Context, pw model.PartWriter, relPath, part string, size int64) (int64, error) {
	n, err := pw.PartSize(ctx, relPath)
	if errors.Is(err, model.ErrPartInTheWay) {
		transport.Log.Add("copy", transport.DirWarn, "COPY "+relPath+": "+err.Error()+", removing it")
		return 0, c.dst.Remove(ctx, part)
	}
	if err != nil || n == 0 {
		return 0, err
	}
	held := fmt.Sprintf("the %s its unfinished copy holds", model.FormatSize(n))
	why := "they are more than the source has"
	if n <= size {
		why = c.partMatches(ctx, relPath, part, n, n == size)
	}
	if why != "" {
		transport.Log.Add("copy", transport.DirWarn, "COPY "+relPath+": not resuming after "+held+", "+why+"; copying from the start")
		return 0, nil
	}
	transport.Log.Add("copy", transport.DirOut, "COPY "+relPath+": resuming after "+held+", the same bytes as the source's by checksum")
	return n, nil
}

// partMatches reports why part's n bytes are not known to be the first n
// bytes of relPath on the source: "" when both sides give them the same
// checksum. A whole part is compared with the whole source, so a source that
// grew since does not match it.
func (c *copier) partMatches(ctx context.Context, relPath, part string, n int64, whole bool) string {
	if !c.req.Scanner.NegotiateChecksum() {
		return "no checksum algorithm works on both sides"
	}
	const noRegion = "the source cannot checksum part of a file"
	caps, err := c.src.Capabilities(ctx)
	if err != nil {
		return "the source: " + err.Error()
	}
	var srcSum string
	if whole {
		srcSum, err = c.src.Checksum(ctx, relPath)
	} else if rc, ok := c.src.(model.RegionChecksummer); ok && caps.RegionSums {
		srcSum, err = rc.ChecksumRange(ctx, relPath, 0, n)
	} else {
		return noRegion
	}
	if err != nil {
		return "the source's checksum: " + err.Error()
	}
	dstSum, err := c.dst.Checksum(ctx, part)
	if err != nil {
		return "the unfinished copy's checksum: " + err.Error()
	}
	if srcSum == "" || srcSum != dstSum {
		return "they differ from the source's"
	}
	return ""
}

// fillPart writes the source into the part from offset to its end, under
// the stall guard: the rest after a checked part, or the whole file. A part
// that changed since it was checked (ErrAppendOffset) is written from the
// start. A part as long as the source was checked whole: nothing to write.
func (c *copier) fillPart(ctx context.Context, relPath, part string, src *model.FileEntry, offset *int64, slotBytes, slotBase *atomic.Int64) error {
	if *offset > 0 && *offset == src.Size {
		slotBytes.Add(src.Size)
		slotBase.Add(src.Size)
		return nil
	}
	mode := model.PartMode(src.Mode)
	return transport.WithStallGuard(ctx, slotBytes, transport.StallTimeout(), func(a context.Context) error {
		if *offset > 0 {
			err := resumeAttempt(a, c.src, c.dst, relPath, part, mode, src, *offset, slotBytes, slotBase, nil)
			if !errors.Is(err, transport.ErrAppendOffset) {
				return err
			}
			transport.Log.Add("copy", transport.DirWarn, "COPY "+relPath+": "+err.Error()+"; copying from the start")
			*offset = 0
		}
		return fullCopyAttempt(a, c.src, c.dst, relPath, part, mode, src, slotBytes)
	})
}

// verifyPart compares a resumed part with the whole source before it takes
// the file's place (--verify-resume): the rest came over a channel that may
// have no integrity of its own (FTP). The start was checked before the
// append, so a difference is in the rest; the part goes, and the next try
// copies from the start.
func (c *copier) verifyPart(ctx context.Context, relPath, part string) error {
	srcSum, err := c.src.Checksum(ctx, relPath)
	if err != nil {
		return err
	}
	dstSum, err := c.dst.Checksum(ctx, part)
	if err != nil {
		return err
	}
	if srcSum == dstSum {
		return nil
	}
	err = fmt.Errorf("%w: %s (the source's checksum is %s, the resumed copy's %s)", errResumeMismatch, relPath, srcSum, dstSum)
	transport.Log.Add("copy", transport.DirWarn, err.Error()+"; copying from the start")
	if rerr := c.dst.Remove(ctx, part); rerr != nil && !errors.Is(rerr, os.ErrNotExist) {
		return fmt.Errorf("%w; removing the resumed copy: %v", err, rerr)
	}
	return err
}
