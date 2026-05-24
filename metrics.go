// Copyright 2026 the comlink authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package comlink

// Phase 8(e): comlink-library metrics.
//
// We use the Prometheus Go client directly. The metric vectors
// are global package-level vars so callers don't have to thread
// a recorder through every layer. Tests run in the same process,
// so each call site is exercised — registration uses
// promauto.With(Registry) into the package's local Registry to
// avoid colliding with the default registry that production apps
// will typically have already.
//
// Apps (like comlink-kvd) get the registry via MetricsRegistry()
// and hand it to promhttp.HandlerFor for their /metrics endpoint.
// Apps that integrate with a host registry can use the various
// MustRegister-style helpers or instead use prometheus.Gatherers
// to fan-out.
//
// Cardinality discipline: only stable, low-cardinality labels.
// conv_id is the substrate's conversation ID (one per substrate
// per process — bounded). Replica IDs are NEVER label values.

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// metricsRegistry is the package-local prometheus.Registry. Apps
// expose this via /metrics through MetricsRegistry().
var metricsRegistry = prometheus.NewRegistry()

// MetricsRegistry returns the Prometheus registry comlink metrics
// are registered on. Apps that want to expose /metrics typically
// pass it to promhttp.HandlerFor:
//
//	http.Handle("/metrics", promhttp.HandlerFor(comlink.MetricsRegistry(),
//	    promhttp.HandlerOpts{}))
//
// Apps with their own registry can either use this one alongside
// (prometheus.Gatherers{ours, theirs}) or use MetricsCollectors()
// to pull the individual collectors for registration.
func MetricsRegistry() *prometheus.Registry { return metricsRegistry }

var (
	// metricClusterMembers tracks each Cluster's current member
	// count. Updated by Cluster.refreshMetrics on construction
	// and after each membership change.
	metricClusterMembers = promauto.With(metricsRegistry).NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "comlink_cluster_members",
			Help: "Current number of members in the cluster (as observed by this replica).",
		},
		[]string{"cluster_id"},
	)

	// metricSubstrateSubmitted increments on every successful
	// Substrate.Submit (counts what THIS replica submitted, not
	// the total apply count).
	metricSubstrateSubmitted = promauto.With(metricsRegistry).NewCounterVec(
		prometheus.CounterOpts{
			Name: "comlink_substrate_messages_submitted_total",
			Help: "Messages submitted via Substrate.Submit on this replica.",
		},
		[]string{"conv_id"},
	)

	// metricSubstrateApplied increments after every SM.Apply.
	// Counts ALL applies (own + peer-originated).
	metricSubstrateApplied = promauto.With(metricsRegistry).NewCounterVec(
		prometheus.CounterOpts{
			Name: "comlink_substrate_messages_applied_total",
			Help: "Messages delivered to the application StateMachine via the Order layer.",
		},
		[]string{"conv_id"},
	)

	// metricSubstrateApplyDuration records SM.Apply latency.
	// Bucketing is tuned for sub-millisecond to ~seconds range
	// since psync + Order are all in-process.
	metricSubstrateApplyDuration = promauto.With(metricsRegistry).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "comlink_substrate_apply_duration_seconds",
			Help:    "Wall-clock duration of StateMachine.Apply, including frame decode.",
			Buckets: prometheus.ExponentialBucketsRange(50e-6, 1.0, 12),
		},
		[]string{"conv_id"},
	)

	// metricSubstrateSubmitDuration is the end-to-end latency
	// from Submit entry to local Apply completion — what an
	// application caller observes from a blocking Submit.
	metricSubstrateSubmitDuration = promauto.With(metricsRegistry).NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "comlink_substrate_submit_duration_seconds",
			Help:    "End-to-end Substrate.Submit latency (entry to local apply complete).",
			Buckets: prometheus.ExponentialBucketsRange(100e-6, 30.0, 14),
		},
		[]string{"conv_id"},
	)

	// metricMembershipVoteIn / metricMembershipVoteOut count
	// vote outcomes at the system Manager. Useful for catching
	// loss of quorum or persistent disagreement.
	metricMembershipVoteIn = promauto.With(metricsRegistry).NewCounterVec(
		prometheus.CounterOpts{
			Name: "comlink_membership_votein_total",
			Help: "Cluster-level VoteIn outcomes initiated on this replica.",
		},
		[]string{"outcome"}, // "accepted", "nacked", "timeout", "error"
	)
	metricMembershipVoteOut = promauto.With(metricsRegistry).NewCounterVec(
		prometheus.CounterOpts{
			Name: "comlink_membership_voteout_total",
			Help: "Cluster-level VoteOut outcomes initiated on this replica.",
		},
		[]string{"outcome"},
	)

	// metricMembershipChange counts MembershipChange callbacks
	// the Manager has fired. Labeled by kind (added/removed).
	metricMembershipChange = promauto.With(metricsRegistry).NewCounterVec(
		prometheus.CounterOpts{
			Name: "comlink_membership_change_events_total",
			Help: "Membership change events observed via the system Manager.",
		},
		[]string{"kind"}, // "added", "removed"
	)
)

// shortConvID returns the first 8 hex chars of a conversation ID
// — enough to identify the substrate in dashboards without
// blowing up label cardinality.
func shortConvID(id ConversationID) string {
	s := id.String()
	if len(s) > 8 {
		return s[:8]
	}
	return s
}
