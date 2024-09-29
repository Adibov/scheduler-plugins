package gitlabrunner

import (
	"fmt"
	"strings"

	"context"
	"math"
	"sort"
	"strconv"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type GitlabRunnerScheduler struct {
	handle framework.Handle
}

const PluginName = "GitlabRunnerScheduler"

func (g *GitlabRunnerScheduler) Name() string {
	return PluginName
}

func (g *GitlabRunnerScheduler) Score(ctx context.Context, state *framework.CycleState, p *v1.Pod, nodeName string) (int64, *framework.Status) {
	nodes, err := g.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}
	workerNodes := make([]*framework.NodeInfo, 0)
	for _, node := range nodes {
		if strings.Contains(node.Node().GetLabels()["kubernetes.io/hostname"], "worker") {
			workerNodes = append(workerNodes, node)
		}
	}
	sort.Slice(workerNodes, func(i, j int) bool {
		return workerNodes[i].Node().Name < workerNodes[j].Node().Name
	})
	if projectIdStr, ok := p.GetAnnotations()["project.runner.gitlab.com/id"]; ok {
		projectId, err := strconv.Atoi(projectIdStr)
		if err != nil {
			return 0, framework.NewStatus(framework.Error, fmt.Sprintf("converting project id %q to int: %v", projectIdStr, err))
		}
		if workerNodes[projectId%len(workerNodes)].Node().Name == nodeName {
			fmt.Printf("Pod %s is scheduled on node %s\n", p.Name, nodeName)
			return framework.MaxNodeScore, nil
		}
	}
	fmt.Printf("Pod %s is not scheduled on node %s\n", p.Name, nodeName)
	return 0, nil
}

func (g *GitlabRunnerScheduler) ScoreExtensions() framework.ScoreExtensions {
	return g
}

func (g *GitlabRunnerScheduler) NormalizeScore(ctx context.Context, state *framework.CycleState, p *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	// Find highest and lowest scores.
	var highest int64 = -math.MaxInt64
	var lowest int64 = math.MaxInt64
	for _, nodeScore := range scores {
		if nodeScore.Score > highest {
			highest = nodeScore.Score
		}
		if nodeScore.Score < lowest {
			lowest = nodeScore.Score
		}
	}

	// Transform the highest to the lowest score range to fit the framework's min to max node score range.
	oldRange := highest - lowest
	newRange := framework.MaxNodeScore - framework.MinNodeScore
	for i, nodeScore := range scores {
		if oldRange == 0 {
			scores[i].Score = framework.MinNodeScore
		} else {
			scores[i].Score = ((nodeScore.Score - lowest) * newRange / oldRange) + framework.MinNodeScore
		}
	}

	return nil
}

// New initializes a new plugin and returns it.
func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &GitlabRunnerScheduler{handle: h}, nil
}
