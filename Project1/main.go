package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	//
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	//RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

/*
SJFPrioritySchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
•	an output writer
•	a title for the chart
•	a slice of processes
*/
func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime       int64
		totalWait         float64
		totalTurnaround   float64
		lastCompletion    float64
		waitingTime       int64
		timeSpent         int64
		incomingProcesses int
		arrivedProcesses  int64
		finishedProcesses int64
		schedule          = make([][]string, len(processes))
		gantt             = make([]TimeSlice, 0)
		//processes that have yet to arrive
		notArrived = make([]Process, len(processes))
		//processes that have arrived but not run
		waiting = make([]Process, len(processes))
		//processes that have finished running
		alreadyRun = make([]Process, len(processes))
	)
	//Organize processes by arrival time
	copy(notArrived, processes)

	timeSpent = 0
	incomingProcesses = len(notArrived)
	arrivedProcesses = 0
	finishedProcesses = 0

	for k := 0; k < len(processes); k++ {
		//sort incoming processes
		for j := 0; j < incomingProcesses; j++ {
			for i := 0; i < incomingProcesses-1; i++ {
				if notArrived[i].ArrivalTime > notArrived[i+1].ArrivalTime {
					notArrived[i], notArrived[i+1] = swap(notArrived[i], notArrived[i+1])
				}
			}
		}

		//receive processes
		for i := 0; i < incomingProcesses; i++ {
			if notArrived[i].ArrivalTime <= timeSpent {
				waiting[arrivedProcesses] = notArrived[i]
				incomingProcesses--
				for j := i; j < incomingProcesses; j++ {
					waiting[j], waiting[j+1] = swap(waiting[j], waiting[j+1])
				}
				arrivedProcesses++
				//need to check current position again
				i--
			}
		}

		//schedule next process
		for i := 0; i < int(arrivedProcesses); i++ {
			for j := 0; j < int(arrivedProcesses-1); j++ {
				if waiting[j].Priority > waiting[j+1].Priority {
					waiting[j], waiting[j+1] = swap(waiting[j], waiting[i+j])
				}
			}
		}

		//run next process
		alreadyRun[finishedProcesses] = waiting[0]
		timeSpent += alreadyRun[finishedProcesses].BurstDuration
		finishedProcesses++
		arrivedProcesses--
		waiting[0], waiting[arrivedProcesses] = swap(waiting[0], waiting[arrivedProcesses])

		//increase priority for waiting processes
		for i := 0; i < int(arrivedProcesses); i++ {
			if waiting[i].Priority > 1 {
				waiting[i].Priority--
			}
		}
	}

	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

/*
SJFSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
•	an output writer
•	a title for the chart
•	a slice of processes
*/
func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime       int64
		totalWait         float64
		totalTurnaround   float64
		lastCompletion    float64
		waitingTime       int64
		timeSpent         int64
		incomingProcesses int
		arrivedProcesses  int64
		finishedProcesses int64
		schedule          = make([][]string, len(processes))
		gantt             = make([]TimeSlice, 0)
		//processes that have yet to arrive
		notArrived = make([]Process, len(processes))
		//processes that have arrived but not run
		waiting = make([]Process, len(processes))
		//processes that have finished running
		alreadyRun = make([]Process, len(processes))
	)

	//Organize processes by arrival time
	copy(notArrived, processes)

	timeSpent = 0
	incomingProcesses = len(notArrived)
	arrivedProcesses = 0
	finishedProcesses = 0

	for k := 0; k < len(processes); k++ {
		//sort incoming processes
		for j := 0; j < incomingProcesses; j++ {
			for i := 0; i < incomingProcesses-1; i++ {
				if notArrived[i].ArrivalTime > notArrived[i+1].ArrivalTime {
					notArrived[i], notArrived[i+1] = swap(notArrived[i], notArrived[i+1])
				}
			}
		}

		//receive processes
		for i := 0; i < incomingProcesses; i++ {
			if notArrived[i].ArrivalTime <= timeSpent {
				waiting[arrivedProcesses] = notArrived[i]
				incomingProcesses--
				for j := i; j < incomingProcesses; j++ {
					waiting[j], waiting[j+1] = swap(waiting[j], waiting[j+1])
				}
				arrivedProcesses++
				//need to check current position again
				i--
			}
		}

		//schedule next process
		for i := 0; i < int(arrivedProcesses); i++ {
			for j := 0; j < int(arrivedProcesses-1); j++ {
				if waiting[j].BurstDuration > waiting[j+1].BurstDuration {
					waiting[j], waiting[j+1] = swap(waiting[j], waiting[i+j])
				}
			}
		}

		//run next process
		alreadyRun[finishedProcesses] = waiting[0]
		timeSpent += alreadyRun[finishedProcesses].BurstDuration
		finishedProcesses++
		arrivedProcesses--
		waiting[0], waiting[arrivedProcesses] = swap(waiting[0], waiting[arrivedProcesses])
	}

	for i := range alreadyRun {
		if alreadyRun[i].ArrivalTime > 0 {
			waitingTime = serviceTime - alreadyRun[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + alreadyRun[i].ArrivalTime

		turnaround := alreadyRun[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := alreadyRun[i].BurstDuration + alreadyRun[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(alreadyRun[i].ProcessID),
			fmt.Sprint(alreadyRun[i].Priority),
			fmt.Sprint(alreadyRun[i].BurstDuration),
			fmt.Sprint(alreadyRun[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += alreadyRun[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   alreadyRun[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(alreadyRun))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

/*
RRSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
•	an output writer
•	a title for the chart
•	a slice of processes
*/
func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime       int64
		totalWait         float64
		totalTurnaround   float64
		lastCompletion    float64
		waitingTime       int64
		timeSpent         int64
		incomingProcesses int
		arrivedProcesses  int64
		finishedProcesses int64
		schedule          = make([][]string, len(processes))
		gantt             = make([]TimeSlice, 0)
		//processes that have yet to arrive
		notArrived = make([]Process, len(processes))
		//processes that have arrived but not run
		waiting = make([]Process, len(processes))
		//remaining burst count for a process
		burstLeft = make([]int64, len(processes))
		//processes that have finished running
		alreadyRun = make([]Process, len(processes))
	)

	copy(notArrived, processes)

	timeSpent = 0
	incomingProcesses = len(notArrived)
	arrivedProcesses = 0
	finishedProcesses = 0
	//sort incoming processes
	for j := 0; j < len(notArrived); j++ {
		for i := 0; i < len(notArrived)-1; i++ {
			if notArrived[i].ArrivalTime > notArrived[i+1].ArrivalTime {
				notArrived[i], notArrived[i+1] = swap(notArrived[i], notArrived[i+1])
			}
		}
	}

	for finishedProcesses = 0; finishedProcesses < int64(len(processes)); {
		//run a process for one time slice
		if arrivedProcesses != 0 {
			//decrease its burst count (separate var), increase timeSpent
			burstLeft[0]--
			timeSpent++

			//move process to the back
			for i := 0; i < int(arrivedProcesses)-1; i++ {
				waiting[i], waiting[i+1] = swap(waiting[i], waiting[i+1])
				burstLeft[i], burstLeft[i+1] = swapInt(burstLeft[i], burstLeft[i+1])
			}

			//check it if is finished

			//if finished, add it to alreadyRun, decrease arrivedProcesses, increase finishedProcesses
			if burstLeft[arrivedProcesses-1] == 0 {
				arrivedProcesses--
				alreadyRun[finishedProcesses] = waiting[arrivedProcesses]
				finishedProcesses++
			}

		}
		//check for new processes
		//receive processes
		for i := 0; i < incomingProcesses; i++ {
			if notArrived[i].ArrivalTime <= timeSpent {
				waiting[arrivedProcesses] = notArrived[i]
				incomingProcesses--
				for j := i; j < incomingProcesses; j++ {
					waiting[j], waiting[j+1] = swap(waiting[j], waiting[j+1])
				}
				arrivedProcesses++
				//need to check current position again
				i--
			}
		}

	}

	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion

//region Additional functions

// Swaps two processes with each other
func swap(p1 Process, p2 Process) (Process, Process) {
	return p2, p1
}

func swapInt(i1 int64, i2 int64) (int64, int64) {
	return i2, i1
}

//endregion
