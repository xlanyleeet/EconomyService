package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	CoinsAddedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "economy_coins_added_total",
		Help: "Total amount of coins added",
	}, []string{"source"})

	TransactionsProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "economy_transactions_processed_total",
		Help: "Total number of economy transactions executed",
	}, []string{"currency", "source"})

	DailyClaimsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "economy_daily_claims_total",
		Help: "Total number of daily bonuses claimed",
	}, []string{"streak_day"})

	ProcessDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "economy_process_duration_seconds",
		Help:    "Latency of processing economy stream events",
		Buckets: prometheus.DefBuckets,
	})
)
