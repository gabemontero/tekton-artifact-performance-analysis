package main

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"os"
	"sort"
	"strings"
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
	tapa.AddCommand(ParseAllThreeLists())
	if err := tapa.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "tapa encountered the following error: %s\n", err.Error())
		os.Exit(1)
	}
}

var prStartTimes = map[string]time.Time{}
var prEndTimes = map[string]time.Time{}
var trStartTimes = map[string]time.Time{}
var trEndTimes = map[string]time.Time{}
var podStartTimes = map[string]time.Time{}
var podEndTimes = map[string]time.Time{}

var prToDuration = map[string]float64{}
var prDurations = []float64{}
var prDurationsMap = map[float64]struct{}{}
var podToDuration = map[string]float64{}
var podDurations = []float64{}
var podDurationsMap = map[float64]struct{}{}
var trToDuration = map[string]float64{}
var trDurations = []float64{}
var trDurationsMap = map[float64]struct{}{}

func ignorePipelineRun(pr *v1beta1.PipelineRun, prFilter string) bool {
	prKey := fmt.Sprintf("%s:%s", pr.Namespace, pr.Name)
	if len(prFilter) > 0 && prKey != prFilter {
		return true
	}
	if !pr.HasStarted() {
		return true
	}
	if !pr.IsDone() {
		return true
	}
	return false
}

func ignoreTaskRun(tr *v1beta1.TaskRun, prFilter string) bool {
	if !tr.HasStarted() {
		return true
	}
	if !tr.IsDone() {
		return true
	}
	trKey := fmt.Sprintf("%s:%s", tr.Namespace, tr.Name)
	if len(prFilter) > 0 && !strings.HasPrefix(trKey, prFilter) {
		return true
	}
	return false
}

func prOwns(kidns, kidname, prns, prname string) bool {
	if kidns != prns {
		return false
	}
	if !strings.HasPrefix(kidname, prname) {
		return false
	}
	return true
}

func ignorePod(pod *corev1.Pod, prFilter string) bool {
	if pod.Status.StartTime == nil {
		return true
	}
	if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
		return true
	}
	_, ok := pod.Labels["tekton.dev/pipelineRun"]
	if !ok {
		return true
	}
	podKey := fmt.Sprintf("%s:%s", pod.Namespace, pod.Name)
	if len(prFilter) > 0 && !strings.HasPrefix(podKey, prFilter) {
		return true
	}
	return false
}

func processPipelineRun(pr *v1beta1.PipelineRun) time.Duration {
	duration := pr.Status.CompletionTime.Sub(pr.Status.StartTime.Time)
	prKey := fmt.Sprintf("%s:%s", pr.Namespace, pr.Name)
	prToDuration[prKey] = duration.Seconds()
	_, ok := prDurationsMap[duration.Seconds()]
	if !ok {
		prDurations = append(prDurations, duration.Seconds())
		prDurationsMap[duration.Seconds()] = struct{}{}
	}
	prStartTimes[prKey] = pr.Status.StartTime.Time
	prEndTimes[prKey] = pr.Status.CompletionTime.Time
	return duration
}

func processTaskRun(tr *v1beta1.TaskRun) time.Duration {
	duration := tr.Status.CompletionTime.Sub(tr.Status.StartTime.Time)
	trKey := fmt.Sprintf("%s:%s", tr.Namespace, tr.Name)
	trToDuration[trKey] = duration.Seconds()
	_, ok := trDurationsMap[duration.Seconds()]
	if !ok {
		trDurations = append(trDurations, duration.Seconds())
		trDurationsMap[duration.Seconds()] = struct{}{}
	}
	trStartTimes[trKey] = tr.Status.StartTime.Time
	trEndTimes[trKey] = tr.Status.CompletionTime.Time
	return duration
}

func processPod(pod *corev1.Pod) time.Duration {
	var terimnatedTime time.Time
	for _, status := range pod.Status.ContainerStatuses {
		terminated := status.State.Terminated
		if terminated != nil {
			if terminated.FinishedAt.Time.After(terimnatedTime) {
				terimnatedTime = terminated.FinishedAt.Time
			}
		}
	}
	duration := terimnatedTime.Sub(pod.Status.StartTime.Time)
	podKey := fmt.Sprintf("%s:%s", pod.Namespace, pod.Name)
	podToDuration[podKey] = duration.Seconds()
	podStartTimes[podKey] = pod.Status.StartTime.Time
	podEndTimes[podKey] = terimnatedTime
	_, ok := podDurationsMap[duration.Seconds()]
	if !ok {
		podDurations = append(podDurations, duration.Seconds())
		podDurationsMap[duration.Seconds()] = struct{}{}
	}
	return duration
}

func determinePRConcurrency(prKey string) int {
	return innerConcurrency(prKey, prStartTimes, prEndTimes)
}

func determineTRConcurrency(trKey string) int {
	return innerConcurrency(trKey, trStartTimes, trEndTimes)
}

func determinePodConcurrency(prKey string) int {
	return innerConcurrency(prKey, podStartTimes, podEndTimes)
}

func innerConcurrency(key string, starts map[string]time.Time, ends map[string]time.Time) int {
	st, _ := starts[key]
	en, _ := ends[key]
	total := 1
	for k, start := range starts {
		if k == key {
			continue
		}
		end, _ := ends[k]
		if start.Equal(st) && end.Equal(en) {
			total++
			continue
		}
		if start.Before(en) && end.After(st) {
			total++
		}
	}
	return total
}

func parsePipelineRunList(fileName, prFilter string) ([]string, []float64, []int, bool) {
	buf, err := os.ReadFile(fileName)
	if err != nil {
		return []string{fmt.Sprintf("ERROR: problem reading file %s: %s\n", fileName, err.Error())}, nil, nil, false
	}
	prList := &v1beta1.PipelineRunList{}
	err = json.Unmarshal(buf, prList)
	if err != nil {
		return []string{fmt.Sprintf("ERROR: file %s not marshalling into a PipelineRun list: %s\n", fileName, err.Error())}, nil, nil, false
	}

	for _, pr := range prList.Items {
		if ignorePipelineRun(&pr, prFilter) {
			continue
		}
		processPipelineRun(&pr)
	}
	sort.Float64s(prDurations)
	retS := []string{}
	retF := []float64{}
	retI := []int{}
	for _, duration := range prDurations {
		for key, value := range prToDuration {
			if value == duration {
				retS = append(retS, key)
				retF = append(retF, value)
				retI = append(retI, determinePRConcurrency(key))
			}
		}
	}
	return retS, retF, retI, true
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
			retS, retF, retI, ok := parsePipelineRunList(fileName, "")
			w := os.Stdout
			if !ok {
				w = os.Stderr
				for _, str := range retS {
					fmt.Fprintf(w, str)
				}
			}
			for i, key := range retS {
				fmt.Fprintf(w, "PipelineRun %s\t\ttook %v seconds with concurrency at %d\n", key, retF[i], retI[i])
			}
		},
	}
	return parsePRList
}

func parsePodList(fileName, prFilter string) ([]string, []float64, []int, bool) {
	buf, err := os.ReadFile(fileName)
	if err != nil {
		return []string{fmt.Sprintf("ERROR: problem reading file %s: %s\n", fileName, err.Error())}, nil, nil, false
	}
	podList := &corev1.PodList{}
	err = json.Unmarshal(buf, podList)
	if err != nil {
		return []string{fmt.Sprintf("ERROR: file %s not marshalling into a Pod list: %s\n", fileName, err.Error())}, nil, nil, false
	}

	for _, pod := range podList.Items {
		if ignorePod(&pod, prFilter) {
			continue
		}

		processPod(&pod)
	}
	sort.Float64s(podDurations)
	retS := []string{}
	retF := []float64{}
	retI := []int{}
	for _, duration := range podDurations {
		for key, value := range podToDuration {
			if value == duration {
				retS = append(retS, key)
				retF = append(retF, value)
				retI = append(retI, determinePodConcurrency(key))
			}
		}
	}
	return retS, retF, retI, true
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
			retS, retF, retI, ok := parsePodList(fileName, "")
			w := os.Stdout
			if !ok {
				w = os.Stderr
				for _, str := range retS {
					fmt.Fprintf(w, str)
				}
				return
			}
			for i, key := range retS {
				fmt.Fprintf(w, "Pod %s\t\ttook %v seconds concurrency %d\n", key, retF[i], retI[i])
			}
		},
	}
	return parseTRList
}

func parseTaskRunList(fileName, prFilter string) ([]string, []float64, []int, bool) {
	buf, err := os.ReadFile(fileName)
	if err != nil {
		return []string{fmt.Sprintf("ERROR: problem reading file %s: %s\n", fileName, err.Error())}, nil, nil, false
	}
	trList := &v1beta1.TaskRunList{}
	err = json.Unmarshal(buf, trList)
	if err != nil {
		return []string{fmt.Sprintf("ERROR: file %s not marshalling into a TaskRun list: %s\n", fileName, err.Error())}, nil, nil, false
	}

	for _, tr := range trList.Items {
		if ignoreTaskRun(&tr, prFilter) {
			continue
		}

		processTaskRun(&tr)
	}
	sort.Float64s(trDurations)
	retS := []string{}
	retF := []float64{}
	retI := []int{}
	for _, duration := range trDurations {
		for key, value := range trToDuration {
			if value == duration {
				retS = append(retS, key)
				retF = append(retF, value)
				retI = append(retI, determineTRConcurrency(key))
			}
		}
	}
	return retS, retF, retI, true
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
			retS, retF, retI, ok := parseTaskRunList(fileName, "")
			w := os.Stdout
			if !ok {
				w = os.Stderr
				for _, str := range retS {
					fmt.Fprintf(w, str)
				}
				return
			}
			for i, key := range retS {
				fmt.Fprintf(w, "TaskRun %s\t\ttook %v seconds concurrency %d\n", key, retF[i], retI[i])
			}
		},
	}
	return parseTRList
}

func ParseAllThreeLists() *cobra.Command {
	allList := &cobra.Command{
		Use:   "all <pr file location> <tr file location> <pod file location> [<options>]",
		Short: "Parse a list of PipelineRuns, their TaskRuns, and their Pods, for various statistics",
		Long:  "Parse a list of PipelineRuns, their TaskRuns, and their Pods, for various statistics",
		Example: `
`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) < 3 {
				fmt.Fprintf(os.Stderr, "ERROR: not enough arguments: %s\n", cmd.Use)
				return
			}
			prFileName := args[0]
			trFileName := args[1]
			podFileName := args[2]

			retS1, retF1, retI1, ok1 := parsePipelineRunList(prFileName, "")
			if !ok1 {
				for _, s := range retS1 {
					fmt.Fprintf(os.Stderr, s)
				}
				return
			}
			retS2, retF2, retI2, ok2 := parseTaskRunList(trFileName, "")
			if !ok2 {
				for _, s := range retS2 {
					fmt.Fprintf(os.Stderr, s)
				}
				return
			}
			retS3, retF3, retI3, ok3 := parsePodList(podFileName, "")
			if !ok3 {
				for _, s := range retS3 {
					fmt.Fprintf(os.Stderr, s)
				}
				return
			}

			for i, prkey := range retS1 {
				prDuration := retF1[i]
				prConcurency := retI1[i]

				totalTRDuration := float64(0)
				maxTRConcurrency := 0
				for ii, trKey := range retS2 {
					if !strings.HasPrefix(trKey, prkey) {
						continue
					}
					totalTRDuration = totalTRDuration + retF2[ii]
					if retI2[ii] > maxTRConcurrency {
						maxTRConcurrency = retI2[ii]
					}
				}
				totalPodDuration := float64(0)
				maxPodConcurrency := 0
				for iii, podKey := range retS3 {
					if !strings.HasPrefix(podKey, prkey) {
						continue
					}
					totalPodDuration = totalPodDuration + retF3[iii]
					maxPodConcurrency = maxPodConcurrency + retI3[iii]
					if retI3[iii] > maxPodConcurrency {
						maxPodConcurrency = retI3[iii]
					}
				}

				fmt.Fprintf(os.Stdout, "PipelineRun %s\t\t took %v seconds with pr concurrency %d with taskruns %v seconds delta %v percent %f taskrun max concurrency %d pods %v seconds delta %v percent %f pod max concurrency %d\n",
					prkey,
					prDuration,
					prConcurency,
					totalTRDuration,
					prDuration-totalTRDuration,
					totalTRDuration/prDuration,
					maxTRConcurrency,
					totalPodDuration,
					prDuration-totalPodDuration,
					totalPodDuration/prDuration,
					maxPodConcurrency)
			}

		},
	}
	return allList
}
