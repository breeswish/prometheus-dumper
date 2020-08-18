package dump

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/sirupsen/logrus"
	"github.com/otiai10/copy"
)

func Dump(snapshotDir string, baseDumpDir string, minTs int64, maxTs int64) (*string, error) {
	if minTs >= maxTs {
		return nil, fmt.Errorf("minTs must be < maxTs")
	}

	baseDumpDir, err := filepath.Abs(baseDumpDir)
	if err != nil {
		return nil, fmt.Errorf("resolve dump directory failed: %w", err)
	}

	blockDirs, err := blockDirs(snapshotDir)
	if err != nil {
		return nil, fmt.Errorf("read snapshot blocks failed: %w", err)
	}

	dumpDir := filepath.Join(baseDumpDir, fmt.Sprintf("%s-%x",
		time.Now().UTC().Format("20060102T150405Z0700"),
		rand.Int()))
	if err := os.MkdirAll(dumpDir, 0777); err != nil {
		return nil, fmt.Errorf("create dump directory failed: %w", err)
	}

	for _, blockDir := range blockDirs {
		block, err := tsdb.OpenBlock(nil, blockDir, nil)
		if err != nil {
			return nil, fmt.Errorf("block is corrupted %s: %w", blockDir, err)
		}
		if !block.OverlapsClosedInterval(minTs, maxTs) {
			// Ignore blocks that does not overlap
			continue
		}

		lf := logrus.WithFields(logrus.Fields{
			"block": filepath.Base(blockDir),
			"minTs": time.Unix(0, block.MinTime() * int64(time.Millisecond)).Format(time.RFC3339),
			"maxTs": time.Unix(0, block.MaxTime() * int64(time.Millisecond)).Format(time.RFC3339),
		})
		lf.Info("Processing block")

		if block.MinTime() >= minTs && block.MaxTime() < maxTs {
			// If the block is fully contained by the range, just create a snapshot
			lf.Info("Compaction skipped, copying...")
			if err := copy.Copy(blockDir, filepath.Join(dumpDir, filepath.Base(blockDir))); err != nil {
				return nil, fmt.Errorf("copy block failed: %w", err)
			}
		} else {
			// Otherwise, we need to rewrite the block
			lf.Info("Compacting block...")
			if err := writeBlock(block, dumpDir, minTs, maxTs); err != nil {
				return nil, fmt.Errorf("dump block failed: %w", err)
			}
		}
	}

	return &dumpDir, nil
}

func blockDirs(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var dirs []string

	for _, fi := range files {
		if isBlockDir(fi) {
			dirs = append(dirs, filepath.Join(dir, fi.Name()))
		}
	}
	return dirs, nil
}

func isBlockDir(fi os.FileInfo) bool {
	if !fi.IsDir() {
		return false
	}
	_, err := ulid.ParseStrict(fi.Name())
	return err == nil
}
//
//func addTombstone(stones *tombstones.MemTombstones, ir tsdb.IndexReader, postings index.Postings, minTs int64, maxTs int64) error {
//	var lset labels.Labels
//	var chks []chunks.Meta
//
//Outer:
//	for postings.Next() {
//		err := ir.Series(postings.At(), &lset, &chks)
//		if err != nil {
//			return err
//		}
//
//		for _, chk := range chks {
//			if chk.OverlapsClosedInterval(minTs, maxTs) {
//				tmin, tmax := clampInterval(minTs, maxTs, chks[0].MinTime, chks[len(chks)-1].MaxTime)
//				stones.AddInterval(postings.At(), tombstones.Interval{Mint: tmin, Maxt: tmax})
//				continue Outer
//			}
//		}
//	}
//
//	if postings.Err() != nil {
//		return postings.Err()
//	}
//
//	return nil
//}

func writeBlock(pb *tsdb.Block, dir string, minTs int64, maxTs int64) error {
	//matcher, err := labels.NewMatcher(labels.MatchRegexp, "__name__", ".*")
	//if err != nil {
	//	return fmt.Errorf("create series matcher failed: %w", err)
	//}
	//
	//ir, err := pb.Index()
	//if err != nil {
	//	return fmt.Errorf("create block index reader failed: %w", err)
	//}
	//
	//// Mostly copied from `tsdb.Delete()`, except that it does not write files.
	//p, err := tsdb.PostingsForMatchers(ir, matcher)
	//if err != nil {
	//	return fmt.Errorf("select series failed: %w", err)
	//}
	//
	//stones := tombstones.NewMemTombstones()
	//
	//if minTs > math.MinInt64 {
	//	if err := addTombstone(stones, ir, p, math.MinInt64, minTs); err != nil {
	//		return fmt.Errorf("add tombstone failed: %w", err)
	//	}
	//}
	//if maxTs < math.MaxInt64 {
	//	if err := addTombstone(stones, ir, p, maxTs, math.MaxInt64); err != nil {
	//		return fmt.Errorf("add tombstone failed: %w", err)
	//	}
	//}
	//
	//currentTombstones, _, err := tombstones.ReadTombstones(pb.Dir())
	//if err != nil {
	//	return fmt.Errorf("read block tombstones failed: %w", err)
	//}
	//defer currentTombstones.Close()
	//
	//err = currentTombstones.Iter(func(id uint64, ivs tombstones.Intervals) error {
	//	for _, iv := range ivs {
	//		stones.AddInterval(id, iv)
	//	}
	//	return nil
	//})
	//if err != nil {
	//	return fmt.Errorf("add block tombstones failed: %w", err)
	//}

	opts := tsdb.DefaultOptions()
	opts.RetentionDuration = int64(365 * 24 * time.Hour / time.Millisecond)  // TODO: not sure whether RetentionDuration affects compaction.
	ranges := tsdb.ExponentialBlockRanges(opts.MinBlockDuration, 10, 3)
	compactor, err := tsdb.NewLeveledCompactor(context.Background(), prometheus.DefaultRegisterer, nil, ranges, nil)
	if err != nil {
		return fmt.Errorf("create compactor failed: %w", err)
	}

	minTime := minTs
	if pb.MinTime() > minTime {
		minTime = pb.MinTime()
	}
	maxTime := maxTs
	if pb.MaxTime() < maxTime {
		maxTime = pb.MaxTime()
	}

	meta := pb.Meta()
	_, err = compactor.Write(dir, pb, minTime, maxTime, &meta)
	if err != nil {
		return fmt.Errorf("compact block failed: %w", err)
	}

	return nil
}
//
//var _ tsdb.BlockReader = blockWithMemoryTombstone{}
//
//type blockWithMemoryTombstone struct {
//	block *tsdb.Block
//	tombstone *tombstones.MemTombstones
//}
//
//func (b blockWithMemoryTombstone) Index() (tsdb.IndexReader, error) {
//	return b.block.Index()
//}
//
//func (b blockWithMemoryTombstone) Chunks() (tsdb.ChunkReader, error) {
//	return b.block.Chunks()
//}
//
//func (b blockWithMemoryTombstone) Tombstones() (tombstones.Reader, error) {
//	return b.tombstone, nil
//}
//
//func (b blockWithMemoryTombstone) Meta() tsdb.BlockMeta {
//	return b.block.Meta()
//}
//
//func clampInterval(a, b, mint, maxt int64) (int64, int64) {
//	if a < mint {
//		a = mint
//	}
//	if b > maxt {
//		b = maxt
//	}
//	return a, b
//}
