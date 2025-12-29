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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/percona/percona-toolkit/src/go/mongolib/proto"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"

	"github.com/percona/pmm/agent/agents/mongodb/shared/fingerprinter"
)

// parser parses MongoDB currentOp results into QueryData.
type parser struct {
	l  *logrus.Entry
	fp *fingerprinter.ProfilerFingerprinter
}

// newParser creates a new parser instance.
func newParser(l *logrus.Entry) *parser {
	return &parser{
		l:  l,
		fp: fingerprinter.NewFingerprinter(fingerprinter.DefaultKeyFilters()),
	}
}

// parseCurrentOpResult parses the currentOp result and extracts query data.
func (p *parser) parseCurrentOpResult(result bson.M) []*QueryData {
	inprog, ok := result["inprog"].(bson.A)
	if !ok {
		if p.l != nil {
			p.l.Warn("currentOp result doesn't contain 'inprog' array")
		}
		return nil
	}

	var queries []*QueryData

	for _, item := range inprog {
		op, ok := item.(bson.M)
		if !ok {
			continue
		}

		query := p.parseOperation(op)
		if query != nil {
			queries = append(queries, query)
		}
	}

	return queries
}

// parseOperation parses a single operation from currentOp.
func (p *parser) parseOperation(op bson.M) *QueryData {
	// Skip operations that don't have a namespace or are system operations
	ns, _ := op["ns"].(string)
	if ns == "" || strings.HasPrefix(ns, "local.") || strings.HasPrefix(ns, "admin.") || strings.HasPrefix(ns, "config.") {
		return nil
	}

	// Get operation type
	opType, _ := op["op"].(string)
	if opType == "" || opType == "none" {
		return nil
	}

	// Get opid
	opid, _ := op["opid"].(int32)
	if opid == 0 {
		// Try int64
		opid64, _ := op["opid"].(int64)
		opid = int32(opid64)
	}

	// Get duration (microseconds)
	microsecsRunning, _ := op["microsecs_running"].(int64)
	if microsecsRunning == 0 {
		// Skip operations that haven't been running
		return nil
	}

	// Convert to milliseconds
	duration := float64(microsecsRunning) / 1000.0

	// Get query/command
	var queryJSON string
	var fingerprint string
	if command, ok := op["command"].(bson.M); ok {
		queryBytes, err := json.Marshal(command)
		if err == nil {
			queryJSON = string(queryBytes)
			// Generate fingerprint using shared fingerprinter
			// Convert bson.M to bson.D for SystemProfile
			var cmdD bson.D
			for k, v := range command {
				cmdD = append(cmdD, bson.E{Key: k, Value: v})
			}

			profile := proto.SystemProfile{
				Ns:      ns,
				Op:      opType,
				Command: cmdD,
			}

			if fp, err := p.fp.Fingerprint(profile); err == nil {
				fingerprint = fp.Fingerprint
			}
		}
	}

	// Get planSummary for index detection
	planSummary, _ := op["planSummary"].(string)
	indexUtilized := p.detectIndexUsage(planSummary)

	// Get client info
	client, _ := op["client"].(string)

	// Get waitingForLock
	waitingForLock, _ := op["waitingForLock"].(bool)

	// Get secs_running
	secsRunning, _ := op["secs_running"].(int64)
	if secsRunning == 0 && microsecsRunning > 0 {
		secsRunning = microsecsRunning / 1000000
	}

	// Get docsExamined (RowsExamined)
	// Try "docsExamined" first, then "keysExamined" as fallback
	docsExamined, _ := op["docsExamined"].(int64)
	if docsExamined == 0 {
		keysExamined, _ := op["keysExamined"].(int64)
		docsExamined = keysExamined
	}

	// Get nreturned (RowsSent)
	nreturned, _ := op["nreturned"].(int64)

	return &QueryData{
		OpID:             fmt.Sprintf("%d", opid),
		Namespace:        ns,
		Operation:        opType,
		Query:            queryJSON,
		Fingerprint:      fingerprint,
		Duration:         duration,
		Client:           client,
		WaitingForLock:   waitingForLock,
		IndexUtilized:    indexUtilized,
		PlanSummary:      planSummary,
		SecsRunning:      secsRunning,
		MicrosecsRunning: microsecsRunning,
		RowsExamined:     docsExamined,
		RowsSent:         nreturned,
	}
}

// detectIndexUsage checks if the planSummary indicates index usage.
func (p *parser) detectIndexUsage(planSummary string) bool {
	if planSummary == "" {
		return false
	}

	// IXSCAN indicates index scan was used
	return strings.Contains(planSummary, "IXSCAN")
}
