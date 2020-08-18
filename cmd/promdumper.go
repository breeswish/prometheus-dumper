package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/breeswish/prometheus-dumper/pkg/dump"
)

var (
	dumpMinTs string
	dumpMaxTs string
	dumpDir string
)

func dumpCmdRun(cmd *cobra.Command, args []string) {
	minTs := mustParseTime(dumpMinTs)
	maxTs := mustParseTime(dumpMaxTs)
	if minTs >= maxTs {
		logrus.Fatal("minTs must be less than maxTs")
	}

	dir, err := dump.Dump(args[0], dumpDir, minTs, maxTs)
	if err != nil {
		logrus.Fatal(err)
	}
	logrus.Info("Dump finished")
	fmt.Println(*dir)
}

func main() {
	dumpCmd := &cobra.Command{
		Use:   "promdumper [snapshot_dir]",
		Short: "Create a dump from the snapshot in specified time range",
		Args: cobra.MinimumNArgs(1),
		Run: dumpCmdRun,
	}
	dumpCmd.Flags().StringVar(&dumpMinTs, "min", strconv.Itoa(math.MinInt64),
		fmt.Sprintf("Min timestamp in milliseconds or RFC3339 format (%s)", time.RFC3339))
	dumpCmd.Flags().StringVar(&dumpMaxTs, "max", strconv.Itoa(math.MaxInt64),
		fmt.Sprintf("Max timestamp in milliseconds or RFC3339 format (%s)", time.RFC3339))
	dumpCmd.Flags().StringVarP(&dumpDir, "output", "o", "./dumps", "Dump output directory")

	if err := dumpCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func mustParseTime(s string) int64 {
	if t, err := strconv.ParseInt(s, 10, 64); err == nil {
		return t
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UnixNano() / (int64(time.Millisecond) / int64(time.Nanosecond))
	}
	logrus.Fatal("Cannot parse time %s", s)
	return 0
}
