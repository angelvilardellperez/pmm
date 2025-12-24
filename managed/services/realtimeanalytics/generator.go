// Copyright (C) 2023 Percona LLC
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package realtimeanalytics

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gopkg.in/reform.v1"

	"github.com/percona/pmm/managed/models"
)

// Generator generates mock query data for testing purposes.
type Generator struct {
	l     *logrus.Entry
	store *Store
	db    *reform.DB
}

// NewGenerator creates a new generator.
func NewGenerator(store *Store, db *reform.DB) *Generator {
	return &Generator{
		l:     logrus.WithField("component", "realtimeanalytics/generator"),
		store: store,
		db:    db,
	}
}

// Run starts generating mock data in the background.
func (m *Generator) Run(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	m.l.Info("Generator started")

	for {
		select {
		case <-ctx.Done():
			m.l.Info("Generator stopped")
			return
		case <-ticker.C:
			m.generateAndPush()
		}
	}
}

// generateAndPush generates mock query data for all RTA-enabled services.
func (m *Generator) generateAndPush() {
	// Get all agents with RTA enabled
	agentType := models.RTAMongoDBAgentType
	agents, err := models.FindAgents(m.db.Querier, models.AgentFilters{
		AgentType: &agentType,
	})
	if err != nil {
		m.l.WithError(err).Error("Failed to fetch RTA agents")
		return
	}

	for _, agent := range agents {
		// Skip if RTA is not enabled
		if agent.RTAOptions.IsEmpty() {
			continue
		}

		// Skip if no service ID
		if agent.ServiceID == nil {
			continue
		}

		// Get service information
		service, err := models.FindServiceByID(m.db.Querier, *agent.ServiceID)
		if err != nil {
			m.l.WithError(err).WithField("service_id", *agent.ServiceID).Warn("Failed to fetch service")
			continue
		}

		// Generate 3-7 random queries for this service
		numQueries := rand.Intn(5) + 3 //nolint:gosec
		queries := make([]*QueryData, 0, numQueries)

		for i := 0; i < numQueries; i++ {
			queries = append(queries, m.generateQuery(*agent.ServiceID, service.ServiceName, service.Cluster))
		}

		// Push to store
		m.store.Set(*agent.ServiceID, queries)
		m.l.WithFields(logrus.Fields{
			"service_id": *agent.ServiceID,
			"count":      len(queries),
		}).Debug("Generated mock queries")
	}
}

// generateQuery generates a single mock query.
func (m *Generator) generateQuery(serviceID, serviceName, cluster string) *QueryData {
	operations := []string{"find", "aggregate", "update", "insert", "delete", "findOne"}
	collections := []string{"users", "orders", "products", "sessions", "logs", "metrics"}
	databases := []string{"production", "staging", "analytics", "reporting"}

	op := operations[rand.Intn(len(operations))]     //nolint:gosec
	coll := collections[rand.Intn(len(collections))] //nolint:gosec
	db := databases[rand.Intn(len(databases))]       //nolint:gosec
	duration := 10.0 + rand.Float64()*1000.0         //nolint:gosec
	queryID := uuid.New().String()

	var query string
	var fingerprint string

	switch op {
	case "find":
		query = fmt.Sprintf(`{"find":"%s","filter":{"status":"active","created_at":{"$gte":"2025-01-01"}}}`, coll)
		fingerprint = fmt.Sprintf(`{"find":"%s","filter":{"status":"?","created_at":{"$gte":"?"}}}`, coll)
	case "aggregate":
		query = fmt.Sprintf(`{"aggregate":"%s","pipeline":[{"$match":{"status":"completed"}},{"$group":{"_id":"$user_id","total":{"$sum":"$amount"}}}]}`, coll)
		fingerprint = fmt.Sprintf(`{"aggregate":"%s","pipeline":[{"$match":{"status":"?"}},{"$group":{"_id":"?","total":{"$sum":"?"}}}]}`, coll)
	case "update":
		query = fmt.Sprintf(`{"update":"%s","updates":[{"q":{"_id":"12345"},"u":{"$set":{"status":"processed"}}}]}`, coll)
		fingerprint = fmt.Sprintf(`{"update":"%s","updates":[{"q":{"_id":"?"},"u":{"$set":{"status":"?"}}}]}`, coll)
	case "insert":
		query = fmt.Sprintf(`{"insert":"%s","documents":[{"name":"test","value":42,"timestamp":"2025-12-04T10:00:00Z"}]}`, coll)
		fingerprint = fmt.Sprintf(`{"insert":"%s","documents":[{"name":"?","value":"?","timestamp":"?"}]}`, coll)
	case "delete":
		query = fmt.Sprintf(`{"delete":"%s","deletes":[{"q":{"expired":true},"limit":0}]}`, coll)
		fingerprint = fmt.Sprintf(`{"delete":"%s","deletes":[{"q":{"expired":"?"},"limit":"?"}]}`, coll)
	case "findOne":
		query = fmt.Sprintf(`{"find":"%s","filter":{"_id":"abc123"},"limit":1}`, coll)
		fingerprint = fmt.Sprintf(`{"find":"%s","filter":{"_id":"?"},"limit":"?"}`, coll)
	}

	return &QueryData{
		QueryID:     queryID,
		ServiceID:   serviceID,
		ServiceName: serviceName,
		Cluster:     cluster,
		Namespace:   fmt.Sprintf("%s.%s", db, coll),
		Query:       query,
		Fingerprint: fingerprint,
		Duration:    duration,
		Timestamp:   time.Now(),
	}
}
