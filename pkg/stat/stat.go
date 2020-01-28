package stat

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/deislabs/smi-sdk-go/pkg/apis/metrics"
	"github.com/gogo/protobuf/proto"
	pb "github.com/linkerd/linkerd2/controller/gen/public"
	"github.com/linkerd/linkerd2/pkg/k8s"
	"github.com/linkerd/linkerd2/pkg/protohttp"
	"github.com/prometheus/common/log"
)

const (
	kindTrafficMetrics     = "trafficmetrics"
	kindTrafficMetricsList = "trafficmetricslist"
)

// FetchStatSummary can send the stats summary requests to the smi-metrics APIService endpoint.
func FetchStatSummary(ctx context.Context, k8sAPI *k8s.KubernetesAPI, req *pb.StatSummaryRequest, controlPlaneNamespace string) (*pb.StatSummaryResponse, error) {
	client, err := k8sAPI.NewClient()
	if err != nil {
		return nil, err
	}

	url, err := url.Parse(k8sAPI.Host)
	if err != nil {
		return nil, err
	}

	var (
		// rsp is the response returned to the caller.
		rsp *pb.StatSummaryResponse

		// rspErr contains all the response errors from the APIService.
		// The goroutines will send their errors to rspErr via the
		// rspErrChan channel.
		rspErr     = []string{}
		rspErrChan = make(chan error)

		// stats contains all the stats to be returned to the caller.
		// The goroutines will send their result to the stats via the
		// statsChan channel.
		stats     = []*pb.StatTable{}
		statsChan = make(chan *pb.StatTable)
	)

	// spawn a goroutine to handle the requests.
	// the response will be handled by the following `select`.
	go func() {
		defer func() {
			close(statsChan)
			close(rspErrChan)
		}()

		url.Path = resourceURL(req)
		log.Infof("resource URL: %s", url)

		b, err := proto.Marshal(req)
		if err != nil {
			rspErrChan <- err
			return
		}

		httpReq, err := http.NewRequest(
			http.MethodGet,
			url.String(),
			bytes.NewReader(b),
		)
		if err != nil {
			rspErrChan <- err
			return
		}

		httpRsp, err := client.Do(httpReq)
		if err != nil {
			rspErrChan <- err
			return
		}

		if err := protohttp.CheckIfResponseHasError(httpRsp); err != nil {
			rspErrChan <- err
			return
		}
		defer httpRsp.Body.Close()

		body, err := ioutil.ReadAll(httpRsp.Body)
		if err != nil {
			rspErrChan <- err
			return
		}

		// determine whether it's TrafficMetrics or TrafficMetricsList
		var objType metav1.TypeMeta
		if err := json.Unmarshal(body, &objType); err != nil {
			rspErrChan <- err
			return
		}

		rows := []*pb.StatTable_PodGroup_Row{}
		addMetricsToStats := func(tm metrics.TrafficMetrics) error {
			var (
				basicStats = &pb.BasicStats{}
				tcpStats   = &pb.TcpStats{}
			)

			for _, metric := range tm.Metrics {
				v := metric.Value.ScaledValue(0)

				switch metric.Name {
				case "p99_response_latency":
					basicStats.LatencyMsP99 = uint64(v)
				case "p90_response_latency":
					basicStats.LatencyMsP95 = uint64(v)
				case "p50_response_latency":
					basicStats.LatencyMsP50 = uint64(v)
				case "success_count":
					basicStats.SuccessCount = uint64(v)
				case "failure_count":
					basicStats.FailureCount = uint64(v)
				default:
					return fmt.Errorf("unsupported metric: %s", metric.Name)
				}
			}

			pods, err := listPodsFor(k8sAPI, tm.Resource, true)
			if err != nil {
				return err
			}

			var (
				podStatus   string
				failedCount uint64
				meshedCount uint64
				totalCount  uint64
			)
			if strings.ToLower(tm.Resource.Kind) == k8s.Pod {
				podStatus = k8s.GetPodStatus(pods[0])
			}

			podErrors := map[string]*pb.PodErrors{}
			for _, pod := range pods {
				if pod.Status.Phase == corev1.PodFailed {
					failedCount++
				} else {
					totalCount++
					if k8s.IsMeshed(&pod, controlPlaneNamespace) {
						meshedCount++
					}
				}

				errors := checkErrors(&pod)
				if len(errors) > 0 {
					podErrors[pod.Name] = &pb.PodErrors{Errors: errors}
				}
			}

			rows = append(rows, &pb.StatTable_PodGroup_Row{
				Resource: &pb.Resource{
					Namespace: tm.Resource.Namespace,
					Type:      strings.ToLower(tm.Resource.Kind),
					Name:      tm.Resource.Name,
				},
				TimeWindow:      tm.Interval.Window.String(),
				Stats:           basicStats,
				TcpStats:        tcpStats,
				Status:          podStatus,
				MeshedPodCount:  meshedCount,
				RunningPodCount: totalCount,
				FailedPodCount:  failedCount,
				ErrorsByPod:     podErrors,
			})

			return nil
		}

		switch strings.ToLower(objType.Kind) {
		case kindTrafficMetrics:
			var tm metrics.TrafficMetrics
			if err := json.Unmarshal(body, &tm); err != nil {
				rspErrChan <- err
				return
			}

			if err := addMetricsToStats(tm); err != nil {
				rspErrChan <- err
				return
			}

		case kindTrafficMetricsList:
			var list metrics.TrafficMetricsList
			if err := json.Unmarshal(body, &list); err != nil {
				rspErrChan <- err
				return
			}

			for _, tm := range list.Items {
				if err := addMetricsToStats(*tm); err != nil {
					rspErrChan <- err
					return
				}
			}

		default:
			rspErrChan <- fmt.Errorf("unsupported smi-metrics kind: %s", objType.Kind)
			return
		}

		statsChan <- &pb.StatTable{
			Table: &pb.StatTable_PodGroup_{
				PodGroup: &pb.StatTable_PodGroup{
					Rows: rows,
				},
			},
		}
	}()

	select {
	case stat, ok := <-statsChan:
		if ok {
			stats = append(stats, stat)
		}
	case err, ok := <-rspErrChan:
		if ok {
			rspErr = append(rspErr, err.Error())
		}
	}

	if len(rspErr) > 0 {
		return nil, fmt.Errorf("%s", strings.Join(rspErr, "\n"))
	}

	rsp = &pb.StatSummaryResponse{
		Response: &pb.StatSummaryResponse_Ok_{
			Ok: &pb.StatSummaryResponse_Ok{
				StatTables: stats,
			},
		},
	}

	return rsp, nil
}

func resourceURL(req *pb.StatSummaryRequest) string {
	const baseURL = "/apis/metrics.smi-spec.io/v1alpha1/namespaces"

	res := req.GetSelector().GetResource()
	if res.GetType() == k8s.Namespace {
		return fmt.Sprintf("%s/%s", baseURL, res.GetName())
	}

	return fmt.Sprintf("%s/%s/%s/%s", baseURL, res.GetNamespace(), res.GetType()+"s", res.GetName())
}

// listPodsFor returns all running and pending Pods associated with a given
// Kubernetes object. Use includeFailed to also get failed Pods
func listPodsFor(kubeAPI *k8s.KubernetesAPI, objRef *corev1.ObjectReference, includeFailed bool) ([]corev1.Pod, error) {
	listPods := func(podSelector labels.Selector, owner metav1.Object) ([]corev1.Pod, error) {
		opts := metav1.ListOptions{
			LabelSelector: podSelector.String(),
		}
		pods, err := kubeAPI.CoreV1().Pods(objRef.Namespace).List(opts)
		if err != nil {
			return nil, err
		}

		pendingOrRunningPods := []corev1.Pod{}
		for _, pod := range pods.Items {
			if isPendingOrRunning(pod) || (includeFailed && isFailed(pod)) {
				if owner == nil || metav1.IsControlledBy(&pod, owner) {
					pendingOrRunningPods = append(pendingOrRunningPods, pod)
				}
			}
		}

		return pendingOrRunningPods, nil
	}

	switch strings.ToLower(objRef.Kind) {
	case k8s.Namespace:
		return listPods(labels.Everything(), nil)

	case k8s.CronJob:
		cronJob, err := kubeAPI.BatchV1beta1().CronJobs(objRef.Namespace).Get(objRef.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		listJobsOpts := metav1.ListOptions{
			LabelSelector: labels.Everything().String(),
		}
		jobs, err := kubeAPI.BatchV1().Jobs(cronJob.Namespace).List(listJobsOpts)
		if err != nil {
			return nil, err
		}

		var pods []corev1.Pod
		for _, job := range jobs.Items {
			if metav1.IsControlledBy(&job, cronJob) {
				jobRef := corev1.ObjectReference{
					APIVersion: job.APIVersion,
					Kind:       job.Kind,
					Name:       job.Name,
					Namespace:  job.Namespace,
				}

				jobPods, err := listPodsFor(kubeAPI, &jobRef, includeFailed)
				if err != nil {
					return nil, err
				}

				pods = append(pods, jobPods...)
			}
		}

		return pods, nil

	case k8s.DaemonSet:
		ds, err := kubeAPI.AppsV1().DaemonSets(objRef.Namespace).Get(objRef.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		selector, err := metav1.LabelSelectorAsSelector(ds.Spec.Selector)
		if err != nil {
			return nil, err
		}

		return listPods(selector, ds)

	case k8s.Deployment:
		deploy, err := kubeAPI.AppsV1().Deployments(objRef.Namespace).Get(objRef.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		opts := metav1.ListOptions{
			LabelSelector: metav1.FormatLabelSelector(deploy.Spec.Selector),
		}
		replicasets, err := kubeAPI.AppsV1().ReplicaSets(objRef.Namespace).List(opts)
		if err != nil {
			return nil, err
		}

		var pods []corev1.Pod
		for _, rs := range replicasets.Items {
			if metav1.IsControlledBy(&rs, deploy) {
				rsRef := &corev1.ObjectReference{
					Kind:      k8s.ReplicaSet,
					Name:      rs.Name,
					Namespace: rs.Namespace,
				}
				podsRS, err := listPodsFor(kubeAPI, rsRef, includeFailed)
				if err != nil {
					return nil, err
				}
				pods = append(pods, podsRS...)
			}
		}
		return pods, nil

	case k8s.ReplicaSet:
		rs, err := kubeAPI.AppsV1().ReplicaSets(objRef.Namespace).Get(objRef.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		selector, err := metav1.LabelSelectorAsSelector(rs.Spec.Selector)
		if err != nil {
			return nil, err
		}

		return listPods(selector, rs)

	case k8s.Job:
		job, err := kubeAPI.BatchV1().Jobs(objRef.Namespace).Get(objRef.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		selector, err := metav1.LabelSelectorAsSelector(job.Spec.Selector)
		if err != nil {
			return nil, err
		}

		return listPods(selector, job)

	case k8s.ReplicationController:
		rc, err := kubeAPI.CoreV1().ReplicationControllers(objRef.Namespace).Get(objRef.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		selector := labels.Set(rc.Spec.Selector).AsSelector()
		return listPods(selector, rc)

	case k8s.Service:
		svc, err := kubeAPI.CoreV1().Services(objRef.Namespace).Get(objRef.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		if svc.Spec.Type == corev1.ServiceTypeExternalName {
			return listPods(labels.Nothing(), nil)
		}

		selector := labels.Set(svc.Spec.Selector).AsSelector()
		return listPods(selector, nil)

	case k8s.StatefulSet:
		sts, err := kubeAPI.AppsV1().StatefulSets(objRef.Namespace).Get(objRef.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		selector, err := metav1.LabelSelectorAsSelector(sts.Spec.Selector)
		if err != nil {
			return nil, err
		}

		return listPods(selector, sts)

	case k8s.Pod:
		pod, err := kubeAPI.CoreV1().Pods(objRef.Namespace).Get(objRef.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		return []corev1.Pod{*pod}, nil

	default:
		return nil, fmt.Errorf("Cannot get object selector: %v", objRef.Kind)
	}

	return nil, nil
}

func isPendingOrRunning(pod corev1.Pod) bool {
	pending := pod.Status.Phase == corev1.PodPending
	running := pod.Status.Phase == corev1.PodRunning
	terminating := pod.DeletionTimestamp != nil
	return (pending || running) && !terminating
}

func isFailed(pod corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodFailed
}

func checkErrors(pod *corev1.Pod) []*pb.PodErrors_PodError {
	errors := []*pb.PodErrors_PodError{}
	addErrors := func(status corev1.ContainerStatus) {
		if !status.Ready {
			var reason, message string
			if status.State.Waiting != nil {
				reason = status.State.Waiting.Reason
				message = status.State.Waiting.Message
			}

			if status.State.Terminated != nil && (status.State.Terminated.ExitCode != 0 || status.State.Terminated.Signal != 0) {
				reason = status.State.Terminated.Reason
				message = status.State.Terminated.Message
			}

			if status.LastTerminationState.Waiting != nil {
				reason = status.LastTerminationState.Waiting.Reason
				message = status.LastTerminationState.Waiting.Message
			}

			if status.LastTerminationState.Terminated != nil {
				reason = status.LastTerminationState.Terminated.Reason
				message = status.LastTerminationState.Terminated.Message
			}

			err := &pb.PodErrors_PodError{
				Error: &pb.PodErrors_PodError_Container{
					Container: &pb.PodErrors_PodError_ContainerError{
						Message:   message,
						Container: status.Name,
						Image:     status.Image,
						Reason:    reason,
					},
				},
			}
			errors = append(errors, err)
		}
	}

	for _, status := range pod.Status.ContainerStatuses {
		addErrors(status)
	}
	return errors
}
