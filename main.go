package main

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"os"
	"sort"
	"time"
)

func main() {
	tapa := &cobra.Command{
		Use: "tapa",
		Long: "Tekton Artifact Performance Analysis (tapa) is a tool that inspects lists of Tekton objects or their underlying Pods\n" +
			" and determines time spent on particular units of work, or the amount of time between the execution of pieces of work.",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Help()
		},
	}
	tapa.AddCommand(ParsePipelineRunList())
	tapa.AddCommand(ParseTaskRunList())
	tapa.AddCommand(ParsePodList())
	if err := tapa.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "tapa encountered the following error: %s\n", err.Error())
		os.Exit(1)
	}
}

func ParsePipelineRunList() *cobra.Command {
	parsePRList := &cobra.Command{
		Use:   "prlist <file location> [<options>]",
		Short: "Parse a list of Tekton PipelineRuns for various statistics",
		Long:  "Parse a list of Tekton PipelineRuns for various statistics",
		Example: `
`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				fmt.Fprintf(os.Stderr, "ERROR: not enough arguments: %s\n", cmd.Use)
				return
			}
			fileName := args[0]
			buf, err := os.ReadFile(fileName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: problem reading file %s: %s\n", fileName, err.Error())
				return
			}
			prList := &v1beta1.PipelineRunList{}
			err = json.Unmarshal(buf, prList)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: file %s not marshalling into a PipelineRun list: %s\n", fileName, err.Error())
				return
			}

			prToDuration := map[string]float64{}
			durations := []float64{}
			durationsMap := map[float64]struct{}{}
			for _, pr := range prList.Items {
				if !pr.HasStarted() {
					//TODO track not started
					continue
				}

				if !pr.IsDone() {
					// TODO track not completed
					continue
				}

				duration := pr.Status.CompletionTime.Sub(pr.Status.StartTime.Time)
				prToDuration[fmt.Sprintf("%s:%s", pr.Namespace, pr.Name)] = duration.Seconds()
				_, ok := durationsMap[duration.Seconds()]
				if !ok {
					durations = append(durations, duration.Seconds())
					durationsMap[duration.Seconds()] = struct{}{}
				}
			}
			sort.Float64s(durations)
			for _, duration := range durations {
				for key, value := range prToDuration {
					if value == duration {
						fmt.Fprintf(os.Stdout, "PipelineRun %s\t\ttook %v seconds\n", key, value)
					}
				}
			}
		},
	}
	return parsePRList
}

func ParsePodList() *cobra.Command {
	parseTRList := &cobra.Command{
		Use:   "podlist <file location> [<options>]",
		Short: "Parse a list of Pods for various statistics",
		Long:  "Parse a list of Pods for various statistics",
		Example: `
`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				fmt.Fprintf(os.Stderr, "ERROR: not enough arguments: %s\n", cmd.Use)
				return
			}
			fileName := args[0]
			buf, err := os.ReadFile(fileName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: problem reading file %s: %s\n", fileName, err.Error())
				return
			}
			podList := &corev1.PodList{}
			err = json.Unmarshal(buf, podList)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: file %s not marshalling into a Pod list: %s\n", fileName, err.Error())
				return
			}

			podToDuration := map[string]float64{}
			durations := []float64{}
			durationsMap := map[float64]struct{}{}
			for _, pod := range podList.Items {
				if pod.Status.StartTime == nil {
					//TODO track not started
					continue
				}

				if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
					//TODO track still in progress
					continue
				}

				var duration time.Duration

				for _, status := range pod.Status.ContainerStatuses {
					terminated := status.State.Terminated
					if terminated != nil {
						duration = terminated.FinishedAt.Sub(pod.Status.StartTime.Time)
					}
				}

				podToDuration[fmt.Sprintf("%s:%s", pod.Namespace, pod.Name)] = duration.Seconds()
				_, ok := durationsMap[duration.Seconds()]
				if !ok {
					durations = append(durations, duration.Seconds())
					durationsMap[duration.Seconds()] = struct{}{}
				}
			}
			sort.Float64s(durations)
			for _, duration := range durations {
				for key, value := range podToDuration {
					if value == duration {
						fmt.Fprintf(os.Stdout, "Pod %s\t\ttook %v seconds\n", key, value)
					}
				}
			}
		},
	}
	return parseTRList
}

func ParseTaskRunList() *cobra.Command {
	parseTRList := &cobra.Command{
		Use:   "trlist <file location> [<options>]",
		Short: "Parse a list of TaskRun for various statistics",
		Long:  "Parse a list of TaskRun for various statistics",
		Example: `
`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				fmt.Fprintf(os.Stderr, "ERROR: not enough arguments: %s\n", cmd.Use)
				return
			}
			fileName := args[0]
			buf, err := os.ReadFile(fileName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: problem reading file %s: %s\n", fileName, err.Error())
				return
			}
			trList := &v1beta1.TaskRunList{}
			err = json.Unmarshal(buf, trList)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: file %s not marshalling into a TaskRun list: %s\n", fileName, err.Error())
				return
			}

			prToDuration := map[string]float64{}
			durations := []float64{}
			durationsMap := map[float64]struct{}{}
			for _, tr := range trList.Items {
				if !tr.HasStarted() {
					//TODO track not startedss
					continue
				}

				if !tr.IsDone() {
					// TODO track not completed
					continue
				}

				duration := tr.Status.CompletionTime.Sub(tr.Status.StartTime.Time)
				prToDuration[fmt.Sprintf("%s:%s", tr.Namespace, tr.Name)] = duration.Seconds()
				_, ok := durationsMap[duration.Seconds()]
				if !ok {
					durations = append(durations, duration.Seconds())
					durationsMap[duration.Seconds()] = struct{}{}
				}
			}
			sort.Float64s(durations)
			for _, duration := range durations {
				for key, value := range prToDuration {
					if value == duration {
						fmt.Fprintf(os.Stdout, "TaskRun %s\t\ttook %v seconds\n", key, value)
					}
				}
			}
		},
	}
	return parseTRList
}
