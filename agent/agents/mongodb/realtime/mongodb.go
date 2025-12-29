// Copyright (C) 2023 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package realtime runs Real-Time Analytics Agent for MongoDB.
package realtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/percona/pmm/agent/agents"
	inventoryv1 "github.com/percona/pmm/api/inventory/v1"
	rtav1 "github.com/percona/pmm/api/realtimeanalytics/v1"
)

// QueryData represents a single query from currentOp.
type QueryData struct {
	OpID             string
	Namespace        string
	Operation        string
	Query            string // Raw query JSON
	Fingerprint      string
	Duration         float64 // milliseconds
	Client           string
	WaitingForLock   bool
	IndexUtilized    bool
	PlanSummary      string
	SecsRunning      int64
	MicrosecsRunning int64
	RowsExamined     int64
	RowsSent         int64
}

// MongoDB collects real-time query data from MongoDB using currentOp().
type MongoDB struct {
	agentID string
	l       *logrus.Entry
	changes chan agents.Change

	mongoDSN        string
	pollingInterval time.Duration
	serviceID       string
	serviceName     string
	cluster         string

	// MongoDB connection
	client *mongo.Client

	// Parser for currentOp results
	parser *parser

	// state
	m        sync.Mutex
	running  bool
	doneChan chan struct{}
	wg       *sync.WaitGroup
}

// Params represent Agent parameters.
type Params struct {
	DSN             string
	AgentID         string
	PollingInterval int32 // in seconds
	ServiceID       string
	ServiceName     string
	Cluster         string
}

// New creates new MongoDB Real-Time Analytics service.
func New(params *Params, l *logrus.Entry) (*MongoDB, error) {
	// if dsn is incorrect we should exit immediately as this is not gonna correct itself
	_, err := connstring.Parse(params.DSN)
	if err != nil {
		return nil, err
	}

	return newMongo(params.DSN, l, params), nil
}

func newMongo(mongoDSN string, l *logrus.Entry, params *Params) *MongoDB {
	pollingInterval := time.Duration(params.PollingInterval) * time.Second
	if pollingInterval <= 0 {
		pollingInterval = 1 * time.Second // Default to 1 second if not specified or invalid
	}

	return &MongoDB{
		agentID:         params.AgentID,
		mongoDSN:        mongoDSN,
		pollingInterval: pollingInterval,
		serviceID:       params.ServiceID,
		serviceName:     params.ServiceName,
		cluster:         params.Cluster,
		l:               l,
		changes:         make(chan agents.Change, 10),
		parser:          newParser(l),
	}
}

// Run collects real-time query data and sends it to the channel until ctx is canceled.
func (m *MongoDB) Run(ctx context.Context) {
	// Panic recovery to prevent agent crashes
	defer func() {
		if r := recover(); r != nil {
			m.l.Errorf("Real-Time Analytics agent panicked and recovered: %v", r)
			m.changes <- agents.Change{Status: inventoryv1.AgentStatus_AGENT_STATUS_STOPPING}
		}
		m.changes <- agents.Change{Status: inventoryv1.AgentStatus_AGENT_STATUS_DONE}
		close(m.changes)
	}()

	m.changes <- agents.Change{Status: inventoryv1.AgentStatus_AGENT_STATUS_STARTING}

	// Create persistent connection
	client, err := m.connect(ctx)
	if err != nil {
		m.l.Errorf("can't connect to MongoDB, reason: %v", err)
		m.changes <- agents.Change{Status: inventoryv1.AgentStatus_AGENT_STATUS_STOPPING}
		return
	}
	m.client = client

	m.doneChan = make(chan struct{})
	m.wg = &sync.WaitGroup{}
	m.wg.Add(1)

	m.m.Lock()
	m.running = true
	m.m.Unlock()

	go m.collectionLoop(ctx)

	m.changes <- agents.Change{Status: inventoryv1.AgentStatus_AGENT_STATUS_RUNNING}

	<-ctx.Done()
	m.changes <- agents.Change{Status: inventoryv1.AgentStatus_AGENT_STATUS_STOPPING}

	m.m.Lock()
	m.running = false
	close(m.doneChan)
	m.m.Unlock()

	m.wg.Wait()

	// Disconnect from MongoDB
	if m.client != nil {
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.client.Disconnect(disconnectCtx); err != nil {
			m.l.Warnf("failed to disconnect from MongoDB: %v", err)
		}
		m.client = nil
	}
}

// collectionLoop is the main collection loop.
func (m *MongoDB) collectionLoop(ctx context.Context) {
	defer m.wg.Done()

	// Add panic recovery
	defer func() {
		if r := recover(); r != nil {
			m.l.Errorf("Collection loop panicked and recovered: %v", r)
		}
	}()

	ticker := time.NewTicker(m.pollingInterval)
	defer ticker.Stop()

	m.l.Debugf("Real-Time Analytics collector started with polling interval %v", m.pollingInterval)

	for {
		select {
		case <-ctx.Done():
			m.l.Info("Collection loop stopped by context cancellation")
			return
		case <-m.doneChan:
			m.l.Info("Collection loop stopped")
			return
		case <-ticker.C:
			queryItems, err := m.collect(ctx)
			if err != nil {
				// Error already logged in collect()
				continue
			}
			if len(queryItems) > 0 {
				m.sendQueries(queryItems)
			}
		}
	}
}

// collect performs one collection cycle and returns query data items.
func (m *MongoDB) collect(ctx context.Context) ([]*rtav1.QueryDataItem, error) {
	// Add timeout for collection
	collectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Run currentOp command
	currentOpResult, err := m.runCurrentOp(collectCtx, m.client)
	if err != nil {
		m.l.Warnf("failed to run currentOp: %v", err)
		// Connection might be dead, try to reconnect
		if m.client != nil {
			m.client.Disconnect(collectCtx) //nolint:errcheck
			m.client = nil
		}
		// Try to reconnect immediately
		client, reconnectErr := m.connect(collectCtx)
		if reconnectErr != nil {
			return nil, fmt.Errorf("failed to reconnect to MongoDB: %w", reconnectErr)
		}
		m.client = client
		return nil, err
	}

	// Parse and process results
	queries := m.parser.parseCurrentOpResult(currentOpResult)

	if len(queries) == 0 {
		return nil, nil
	}

	m.l.Debugf("Collected %d running queries", len(queries))

	// Convert QueryData to QueryDataItem (protobuf)
	queryItems := make([]*rtav1.QueryDataItem, 0, len(queries))
	for _, q := range queries {
		queryID := q.OpID
		if queryID == "" {
			queryID = uuid.New().String()
		}

		queryItems = append(queryItems, &rtav1.QueryDataItem{
			QueryId:      queryID,
			ServiceId:    m.serviceID,
			ServiceName:  m.serviceName,
			Cluster:      m.cluster,
			Namespace:    q.Namespace,
			Query:        q.Query,
			Fingerprint:  q.Fingerprint,
			Duration:     q.Duration,
			RowsExamined: q.RowsExamined,
			RowsSent:     q.RowsSent,
			Timestamp:    timestamppb.Now(),
		})
	}

	return queryItems, nil
}

// sendQueries sends query data items via Changes channel.
func (m *MongoDB) sendQueries(queryItems []*rtav1.QueryDataItem) {
	// Send via Changes channel (non-blocking)
	select {
	case m.changes <- agents.Change{RTAMetrics: queryItems}:
	default:
		m.l.Warnf("Changes channel full, dropping %d queries", len(queryItems))
	}
}

// connect creates a MongoDB client connection.
func (m *MongoDB) connect(ctx context.Context) (*mongo.Client, error) {
	clientOptions := options.Client().
		ApplyURI(m.mongoDSN).
		SetAppName(fmt.Sprintf("pmm-agent-%s", m.agentID)).
		SetConnectTimeout(10 * time.Second).
		SetServerSelectionTimeout(10 * time.Second)

	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}

	// Ping to verify connection
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx) //nolint:errcheck
		return nil, err
	}

	return client, nil
}

// runCurrentOp executes the currentOp command.
func (m *MongoDB) runCurrentOp(ctx context.Context, client *mongo.Client) (bson.M, error) {
	// Run currentOp with filter to get only active operations
	command := bson.D{
		{Key: "currentOp", Value: true},
		{Key: "active", Value: true},
		{Key: "$all", Value: true}, // Include all operations including system operations
	}

	var result bson.M
	err := client.Database("admin").RunCommand(ctx, command).Decode(&result)
	if err != nil {
		return nil, err
	}

	output, _ := bson.MarshalExtJSON(result, true, false)
	fmt.Printf("output: %s\n", output)

	return result, nil
}

// Changes returns channel that should be read until it is closed.
func (m *MongoDB) Changes() <-chan agents.Change {
	return m.changes
}

// Describe implements prometheus.Collector.
func (m *MongoDB) Describe(ch chan<- *prometheus.Desc) { //nolint:revive
	// This method is needed to satisfy interface.
}

// Collect implement prometheus.Collector.
func (m *MongoDB) Collect(ch chan<- prometheus.Metric) { //nolint:revive
	// This method is needed to satisfy interface.
}

// check interfaces.
var (
	_ prometheus.Collector = (*MongoDB)(nil)
)
