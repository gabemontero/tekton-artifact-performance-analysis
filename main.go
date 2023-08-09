package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/tektoncd/pipeline/pkg/apis/pipeline/v1beta1"
	corev1 "k8s.io/api/core/v1"
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
	tapa.PersistentFlags().StringVarP(&outputType, "output-type", "t", OutputTypeText, "output type, one of: text, csv")
	tapa.ParseFlags(os.Args)

	tapa.AddCommand(ParsePipelineRunList())
	tapa.AddCommand(ParseTaskRunList())
	tapa.AddCommand(ParsePodList())
	tapa.AddCommand(ParseAllThreeLists())

	if outputType != OutputTypeText && outputType != OutputTypeCsv {
		tapa.Help()
		fmt.Fprintf(os.Stderr, "Error: Invalid value for output-type: %s\n", outputType)
		os.Exit(1)
	}

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
var containerStartTimes = map[string]time.Time{}
var containerEndTimmes = map[string]time.Time{}

var prToDuration = map[string]float64{}
var prDurations = []float64{}
var prDurationsMap = map[float64]struct{}{}
var podToDuration = map[string]float64{}
var podDurations = []float64{}
var podDurationsMap = map[float64]struct{}{}
var trToDuration = map[string]float64{}
var trDurations = []float64{}
var trDurationsMap = map[float64]struct{}{}
var containerToDuration = map[string]float64{}
var containerDurations = []float64{}
var containerDurationsMap = map[float64]struct{}{}

const (
	OutputTypeText string = "text"
	OutputTypeCsv  string = "csv"
)

var (
	outputType    string = OutputTypeText
	containerOnly bool   = false
)

func processPRFiles(fileName string) (*v1beta1.PipelineRunList, error) {
	var err error
	prList := &v1beta1.PipelineRunList{}
	prList.Items = []v1beta1.PipelineRun{}

	err = filepath.Walk(fileName, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			fmt.Fprintf(os.Stderr, "filepath walk error: %s\n", err.Error())
			return nil
		}
		if !info.IsDir() {
			buf, e := os.ReadFile(path)
			if e != nil {
				fmt.Fprintf(os.Stderr, "problem reading %s: %s\n", path, e.Error())
				return nil
			}
			prl := &v1beta1.PipelineRunList{}
			e = json.Unmarshal(buf, prl)
			if e != nil {
				return nil
			}
			if len(prl.Items) > 0 {
				// unmarshall is not a perfect type filter
				for _, pr := range prl.Items {
					if pr.Kind != "PipelineRun" {
						continue
					}
					prList.Items = append(prList.Items, pr)
				}

			}
		}
		return nil
	})

	return prList, err
}

func processTRFiles(fileName string) (*v1beta1.TaskRunList, error) {
	var err error
	trList := &v1beta1.TaskRunList{}
	trList.Items = []v1beta1.TaskRun{}

	err = filepath.Walk(fileName, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			fmt.Fprintf(os.Stderr, "filepath walk error: %s\n", err.Error())
			return nil
		}
		if !info.IsDir() {
			buf, e := os.ReadFile(path)
			if e != nil {
				fmt.Fprintf(os.Stderr, "problem reading %s: %s\n", path, e.Error())
				return nil
			}
			trl := &v1beta1.TaskRunList{}
			e = json.Unmarshal(buf, trl)
			if e != nil {
				return nil
			}
			if len(trl.Items) > 0 {
				// unmarshall is not a perfect type filter
				for _, tr := range trl.Items {
					if tr.Kind != "TaskRun" {
						continue
					}
					trList.Items = append(trList.Items, tr)
				}
			}
		}
		return nil
	})

	return trList, err
}

func processPodFiles(fileName string) (*corev1.PodList, error) {
	var err error
	podList := &corev1.PodList{}
	podList.Items = []corev1.Pod{}

	err = filepath.Walk(fileName, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			fmt.Fprintf(os.Stderr, "filepath walk error: %s\n", err.Error())
			return nil
		}
		if !info.IsDir() {
			buf, e := os.ReadFile(path)
			if e != nil {
				fmt.Fprintf(os.Stderr, "problem reading %s: %s\n", path, e.Error())
				return nil
			}
			pl := &corev1.PodList{}
			e = json.Unmarshal(buf, pl)
			if e != nil {
				return nil
			}
			if len(pl.Items) > 0 {
				for _, pod := range pl.Items {
					if pod.Kind != "Pod" {
						continue
					}
					podList.Items = append(podList.Items, pod)
				}
			}
		}
		return nil
	})

	return podList, err
}

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

func processContainers(pod *corev1.Pod) []time.Duration {
	durations := []time.Duration{}
	specNameToIndex := map[string]int{}
	statusNameToIndex := map[string]int{}
	for index, container := range pod.Spec.Containers {
		specNameToIndex[container.Name] = index
	}
	for index, cstatus := range pod.Status.ContainerStatuses {
		statusNameToIndex[cstatus.Name] = index
	}
	for _, cstatus := range pod.Status.ContainerStatuses {
		terminated := cstatus.State.Terminated
		if terminated == nil {
			continue
		}
		// containers are created started concurrently, but k8s/linux "pauses" then "resumes" per spec order
		// so we take that finish time of the prior container if not the first container
		started := terminated.StartedAt.Time
		specIndex, _ := specNameToIndex[cstatus.Name]
		if specIndex != 0 {
			// not first container, get prior container finish time
			priorContainerName := pod.Spec.Containers[specIndex-1].Name
			priorContainerStatusIndex, _ := statusNameToIndex[priorContainerName]
			priorContainerStatus := pod.Status.ContainerStatuses[priorContainerStatusIndex]
			if priorContainerStatus.State.Terminated != nil {
				started = priorContainerStatus.State.Terminated.FinishedAt.Time
			}
		}
		finished := terminated.FinishedAt.Time
		duration := finished.Sub(started)
		ckey := fmt.Sprintf("%s:%s-%s", pod.Namespace, pod.Name, cstatus.Name)
		containerToDuration[ckey] = duration.Seconds()
		containerStartTimes[ckey] = started
		containerEndTimmes[ckey] = finished
		_, ok := containerDurationsMap[duration.Seconds()]
		if !ok {
			containerDurations = append(containerDurations, duration.Seconds())
			containerDurationsMap[duration.Seconds()] = struct{}{}
		}
	}
	return durations
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

func determineContainerConcurrency(ckey string) int {
	return innerConcurrency(ckey, containerStartTimes, containerEndTimmes)
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
	prList, err := processPRFiles(fileName)
	if err != nil {
		return []string{fmt.Sprintf("ERROR: problem reading file %s: %s\n", fileName, err.Error())}, nil, nil, false
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
		Use:   "prlist <file location or directory tree with files> [<options>]",
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
			printList("PipelineRun", retS, retF, retI)
		},
	}
	return parsePRList
}

func parsePodList(fileName, prFilter string) ([]string, []float64, []int, bool) {
	podList, err := processPodFiles(fileName)
	if err != nil {
		return []string{fmt.Sprintf("ERROR: file %s not marshalling into a Pod list: %s\n", fileName, err.Error())}, nil, nil, false
	}

	for _, pod := range podList.Items {
		if ignorePod(&pod, prFilter) {
			continue
		}

		if !containerOnly {
			processPod(&pod)
		} else {
			processContainers(&pod)
		}
	}
	retS := []string{}
	retF := []float64{}
	retI := []int{}
	if !containerOnly {
		sort.Float64s(podDurations)
		for _, duration := range podDurations {
			for key, value := range podToDuration {
				if value == duration {
					retS = append(retS, key)
					retF = append(retF, value)
					retI = append(retI, determinePodConcurrency(key))
				}
			}
		}
	}
	if containerOnly {
		sort.Float64s(containerDurations)
		for _, duration := range containerDurations {
			for key, value := range containerToDuration {
				if value == duration {
					retS = append(retS, key)
					retF = append(retF, value)
					retI = append(retI, determineContainerConcurrency(key))
				}
			}
		}
	}
	return retS, retF, retI, true
}

func ParsePodList() *cobra.Command {
	parsePodListCmd := &cobra.Command{
		Use:   "podlist <file location or directory tree with files> [<options>]",
		Short: "Parse a list of Pods for various statistics",
		Long:  "Parse a list of Pods for various statistics",
		Example: `
# Print just the pods
$ tapa podlist <pod list json/yaml file or directory with files>

# Print just the containers
$ tapa podlist <pod list json/yaml file or directory with files> --containers-only
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
			printList("Pod", retS, retF, retI)
		},
	}
	parsePodListCmd.Flags().BoolVar(&containerOnly, "containers-only", containerOnly,
		"Only list containers and not pods")
	return parsePodListCmd
}

func parseTaskRunList(fileName, prFilter string) ([]string, []float64, []int, bool) {
	trList, err := processTRFiles(fileName)
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
		Use:   "trlist <file location or directory tree with files> [<options>]",
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
			printList("TaskRun", retS, retF, retI)
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
			if len(args) < 1 {
				fmt.Fprintf(os.Stderr, "ERROR: not enough arguments: %s\n", cmd.Use)
				return
			}
			fileStat, err := os.Stat(args[0])
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: could not analyze file %s: %s\n", args[0], err.Error())
				return
			}
			if len(args) < 3 && !fileStat.IsDir() {
				fmt.Fprintf(os.Stderr, "ERROR: not enough arguments: %s\n", cmd.Use)
				return
			}
			var prFileName, trFileName, podFileName string
			if !fileStat.IsDir() {
				prFileName = args[0]
				trFileName = args[1]
				podFileName = args[2]
			} else {
				prFileName, trFileName, podFileName = args[0], args[0], args[0]
			}

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
			printHeader("PipelineRun", "Duration", "Concurrency", "TaskRunsDuration", "TaskRunsDelta", "TaskRunsPercentage", "TaskRunsMaxConcurrency", "PodsDuration", "PodsDelta", "PodsPercentage", "PodsMaxConcurrency")
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
				printLine("PipelineRun %s\t\t took %v seconds with pr concurrency %d with taskruns %v seconds delta %v percent %f taskrun max concurrency %d pods %v seconds delta %v percent %f pod max concurrency %d\n",
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

func printHeader(headers ...string) {
	out := ""
	switch outputType {
	case OutputTypeCsv:
		for _, h := range headers {
			if len(out) > 0 {
				out += fmt.Sprintf(";%s", h)
			} else {
				out = h
			}
		}
		fmt.Fprintln(os.Stdout, out)
	default:
		// text output does not have a header, do not print anything
	}
}

func printLine(format string, values ...any) {
	w := os.Stdout
	out := ""
	switch outputType {
	case OutputTypeCsv:
		for _, v := range values {
			if len(out) > 0 {
				out += fmt.Sprintf(";%v", v)
			} else {
				out = fmt.Sprintf("%v", v)
			}
		}
		fmt.Fprintln(w, out)
	default:
		fmt.Fprintf(w, format, values...)
	}
}

func printList(resource string, keys []string, durations []float64, concurencies []int) {
	printHeader(resource, "Duration", "Concurrency")
	for i, key := range keys {
		printLine(fmt.Sprintf("%s %%s\t\ttook %%v seconds concurrency %%d\n", resource), key, durations[i], concurencies[i])
	}
}
