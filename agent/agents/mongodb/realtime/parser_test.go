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

package realtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestParseOperation(t *testing.T) {
	p := newParser(nil)

	t.Run("valid find operation", func(t *testing.T) {
		op := bson.M{
			"opid":              int32(12345),
			"ns":                "mydb.mycollection",
			"op":                "query",
			"microsecs_running": int64(500000), // 500ms
			"command": bson.M{
				"find":   "mycollection",
				"filter": bson.M{"status": "active"},
			},
			"planSummary":    "IXSCAN { status: 1 }",
			"client":         "127.0.0.1:50123",
			"waitingForLock": false,
		}

		query := p.parseOperation(op)
		require.NotNil(t, query)
		assert.Equal(t, "12345", query.OpID)
		assert.Equal(t, "mydb.mycollection", query.Namespace)
		assert.Equal(t, "query", query.Operation)
		assert.Equal(t, 500.0, query.Duration) // 500ms
		assert.True(t, query.IndexUtilized)
		assert.Equal(t, "IXSCAN { status: 1 }", query.PlanSummary)
		assert.Equal(t, "127.0.0.1:50123", query.Client)
		assert.False(t, query.WaitingForLock)
	})

	t.Run("operation without index", func(t *testing.T) {
		op := bson.M{
			"opid":              int32(12346),
			"ns":                "mydb.mycollection",
			"op":                "query",
			"microsecs_running": int64(100000), // 100ms
			"command": bson.M{
				"find": "mycollection",
			},
			"planSummary": "COLLSCAN",
			"client":      "127.0.0.1:50124",
		}

		query := p.parseOperation(op)
		require.NotNil(t, query)
		assert.False(t, query.IndexUtilized)
		assert.Equal(t, "COLLSCAN", query.PlanSummary)
	})

	t.Run("skip system operations", func(t *testing.T) {
		systemOps := []bson.M{
			{
				"opid":              int32(1),
				"ns":                "local.oplog.rs",
				"op":                "getmore",
				"microsecs_running": int64(1000),
			},
			{
				"opid":              int32(2),
				"ns":                "admin.system.version",
				"op":                "query",
				"microsecs_running": int64(1000),
			},
			{
				"opid":              int32(3),
				"ns":                "config.transactions",
				"op":                "update",
				"microsecs_running": int64(1000),
			},
		}

		for _, op := range systemOps {
			query := p.parseOperation(op)
			assert.Nil(t, query, "System operation should be skipped: %v", op["ns"])
		}
	})

	t.Run("skip operations with no duration", func(t *testing.T) {
		op := bson.M{
			"opid": int32(12347),
			"ns":   "mydb.mycollection",
			"op":   "query",
			// microsecs_running is 0 or missing
			"command": bson.M{
				"find": "mycollection",
			},
		}

		query := p.parseOperation(op)
		assert.Nil(t, query)
	})

	t.Run("skip operations with no operation type", func(t *testing.T) {
		op := bson.M{
			"opid":              int32(12348),
			"ns":                "mydb.mycollection",
			"op":                "none", // or empty
			"microsecs_running": int64(1000),
		}

		query := p.parseOperation(op)
		assert.Nil(t, query)
	})

	t.Run("operation with waitingForLock", func(t *testing.T) {
		op := bson.M{
			"opid":              int32(12349),
			"ns":                "mydb.mycollection",
			"op":                "update",
			"microsecs_running": int64(2000000), // 2 seconds
			"command": bson.M{
				"update": "mycollection",
			},
			"waitingForLock": true,
		}

		query := p.parseOperation(op)
		require.NotNil(t, query)
		assert.True(t, query.WaitingForLock)
		assert.Equal(t, 2000.0, query.Duration)
	})
}

func TestDetectIndexUsage(t *testing.T) {
	p := newParser(nil)

	tests := []struct {
		name        string
		planSummary string
		expected    bool
	}{
		{"with IXSCAN", "IXSCAN { _id: 1 }", true},
		{"with IXSCAN compound", "IXSCAN { status: 1, created_at: -1 }", true},
		{"COLLSCAN only", "COLLSCAN", false},
		{"empty plan", "", false},
		{"mixed with IXSCAN", "FETCH IXSCAN { user_id: 1 }", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := p.detectIndexUsage(tt.planSummary)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseCurrentOpResult(t *testing.T) {
	p := newParser(nil) // Logger can be nil for tests

	t.Run("parse multiple operations", func(t *testing.T) {
		result := bson.M{
			"inprog": bson.A{
				bson.M{
					"opid":              int32(1),
					"ns":                "mydb.users",
					"op":                "query",
					"microsecs_running": int64(100000),
					"command":           bson.M{"find": "users"},
					"planSummary":       "IXSCAN { _id: 1 }",
				},
				bson.M{
					"opid":              int32(2),
					"ns":                "mydb.orders",
					"op":                "update",
					"microsecs_running": int64(200000),
					"command":           bson.M{"update": "orders"},
					"planSummary":       "COLLSCAN",
				},
				// This should be skipped (system namespace)
				bson.M{
					"opid":              int32(3),
					"ns":                "local.oplog.rs",
					"op":                "getmore",
					"microsecs_running": int64(50000),
				},
			},
		}

		queries := p.parseCurrentOpResult(result)
		require.Len(t, queries, 2)

		assert.Equal(t, "1", queries[0].OpID)
		assert.Equal(t, "mydb.users", queries[0].Namespace)
		assert.True(t, queries[0].IndexUtilized)

		assert.Equal(t, "2", queries[1].OpID)
		assert.Equal(t, "mydb.orders", queries[1].Namespace)
		assert.False(t, queries[1].IndexUtilized)
	})

	t.Run("handle invalid inprog", func(t *testing.T) {
		result := bson.M{
			"inprog": "not an array",
		}

		queries := p.parseCurrentOpResult(result)
		assert.Nil(t, queries)
	})

	t.Run("handle missing inprog", func(t *testing.T) {
		result := bson.M{}

		queries := p.parseCurrentOpResult(result)
		assert.Nil(t, queries)
	})
}
