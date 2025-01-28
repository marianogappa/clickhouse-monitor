package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"gonum.org/v1/plot/vg/draw"
	"gonum.org/v1/plot/vg/vgimg"
)

type Measurement struct {
	timestamp     time.Time
	connections   int
	queryDuration time.Duration
}

func main() {
	// Connect to ClickHouse
	// Example: go run . "clickhouse://user:pass@localhost:9440"
	if len(os.Args) < 2 {
		log.Fatal("Please provide ClickHouse DSN as argument")
	}
	opts, err := clickhouse.ParseDSN(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	measurements := []Measurement{}

	log.Println("Starting monitoring. Press Ctrl+C to stop and generate the chart...")

	// Monitor until interrupt
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				measurement := collectMetrics(conn)
				measurements = append(measurements, measurement)
				time.Sleep(300 * time.Millisecond)
			}
		}
	}()

	// Wait for interrupt
	<-sigChan
	done <- true

	log.Println("Stopping monitoring and generating chart...")

	// Generate chart
	if err := generateChart(measurements); err != nil {
		log.Fatal(err)
	}
}

func collectMetrics(conn driver.Conn) Measurement {
	start := time.Now()

	var count int64
	err := conn.QueryRow(context.Background(), "SELECT sum(value) FROM system.metrics WHERE metric IN ('TCPConnection', 'HTTPConnection');").Scan(&count)
	if err != nil {
		log.Printf("Error querying ClickHouse: %v", err)
		return Measurement{timestamp: start}
	}
	fmt.Println("Collected metrics", count)

	connections := count
	duration := time.Since(start)

	return Measurement{
		timestamp:     start,
		connections:   int(connections),
		queryDuration: duration,
	}
}

func generateChart(measurements []Measurement) error {
	if len(measurements) == 0 {
		return fmt.Errorf("no measurements to plot")
	}

	// Prepare data points
	n := len(measurements)
	connectionPts := make(plotter.XYs, n)
	durationPts := make(plotter.XYs, n)

	startTime := measurements[0].timestamp
	for i, m := range measurements {
		t := m.timestamp.Sub(startTime).Seconds()
		connectionPts[i].X = t
		connectionPts[i].Y = float64(m.connections)

		durationPts[i].X = t
		durationPts[i].Y = float64(m.queryDuration.Milliseconds())
	}

	// Create plots array
	const rows, cols = 2, 1
	plots := make([][]*plot.Plot, rows)
	for i := range plots {
		plots[i] = make([]*plot.Plot, cols)
		plots[i][0] = plot.New()
	}

	// Configure first subplot (Connections)
	plots[0][0].Title.Text = "Active Connections"
	plots[0][0].X.Label.Text = "Time (seconds)"
	plots[0][0].Y.Label.Text = "Number of Connections"

	line1, points1, err := plotter.NewLinePoints(connectionPts)
	if err != nil {
		return err
	}
	plots[0][0].Add(line1, points1)
	plots[0][0].Add(plotter.NewGrid())

	// Configure second subplot (Query Duration)
	plots[1][0].Title.Text = "Query Duration"
	plots[1][0].X.Label.Text = "Time (seconds)"
	plots[1][0].Y.Label.Text = "Duration (ms)"

	line2, points2, err := plotter.NewLinePoints(durationPts)
	if err != nil {
		return err
	}
	plots[1][0].Add(line2, points2)
	plots[1][0].Add(plotter.NewGrid())

	// Create the image
	img := vgimg.New(vg.Points(800*1.5), vg.Points(800*2))
	dc := draw.New(img)

	// Configure tiles
	t := draw.Tiles{
		Rows:      rows,
		Cols:      cols,
		PadX:      vg.Millimeter,
		PadY:      vg.Millimeter,
		PadTop:    vg.Points(10),
		PadBottom: vg.Points(10),
		PadLeft:   vg.Points(10),
		PadRight:  vg.Points(10),
	}

	// Draw the plots
	canvases := plot.Align(plots, t, dc)
	for j := 0; j < rows; j++ {
		for i := 0; i < cols; i++ {
			if plots[j][i] != nil {
				plots[j][i].Draw(canvases[j][i])
			}
		}
	}

	// Save to file
	timestamp := time.Now().Format("20060102-150405")
	filename := fmt.Sprintf("clickhouse-metrics-%s.png", timestamp)

	w, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating file: %v", err)
	}
	defer w.Close()

	png := vgimg.PngCanvas{Canvas: img}
	if _, err := png.WriteTo(w); err != nil {
		return fmt.Errorf("error writing PNG: %v", err)
	}

	log.Printf("Chart saved as %s", filename)
	return nil
}
