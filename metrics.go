package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	coinsAddedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "economy_coins_added_total",
		Help: "Total amount of coins added",
	}, []string{"source"})

	transactionsProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "economy_transactions_processed_total",
		Help: "Total number of economy transactions executed",
	}, []string{"currency", "source"})

	dailyClaimsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "economy_daily_claims_total",
		Help: "Total number of daily bonuses claimed",
	}, []string{"streak_day"})

	processDurationHistogram = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "economy_process_duration_seconds",
		Help:    "Latency of processing economy stream events",
		Buckets: prometheus.DefBuckets,
	})
)
