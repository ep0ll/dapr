/*
Copyright 2025 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fork

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/dapr/pkg/runtime/wfengine/state"
	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
)

// setRerunTraceContext assigns each rerun a unique workflow span. When
// preserveTraceContext is false it also starts a new root trace. When true,
// the source ExecutionStarted traceparent is retained as the parent context,
// but the workflow span ID is regenerated to avoid duplicate span identities
// between the source and rerun instances.
func setRerunTraceContext(s *state.State, preserveTraceContext bool) error {
	start := cloneExecutionStartedEvent(s)
	if start == nil {
		return errors.New("workflow history has no execution-started event")
	}

	// An untraced workflow must remain untraced, even when preservation was
	// requested. Clear any stale span ID and trace context from pending work.
	if start.ParentTraceContext == nil {
		start.WorkflowSpanID = nil
		setPendingTraceContext(s, nil)
		return nil
	}

	parentTraceContext := start.ParentTraceContext
	if !preserveTraceContext {
		parentTraceContext = helpers.NewRootTraceContext()
		if parentTraceContext == nil {
			return errors.New("failed to generate a new root trace context")
		}
	}

	parentSpanContext, err := helpers.SpanContextFromTraceContext(parentTraceContext)
	if err != nil {
		return fmt.Errorf("failed to parse rerun parent trace context: %w", err)
	}
	if !parentSpanContext.IsValid() {
		return errors.New("rerun parent trace context is invalid")
	}

	workflowSpanID, err := newWorkflowSpanID()
	if err != nil {
		return err
	}

	start.ParentTraceContext = parentTraceContext
	start.WorkflowSpanID = wrapperspb.String(workflowSpanID)

	// startOrResumeWorkflowSpan rehydrates this span ID for the rerun. Use
	// that same context for activities and child workflows that are re-driven
	// from the forked inbox, matching the context used by newly scheduled work.
	workflowTraceContext := &protos.TraceContext{
		TraceParent: fmt.Sprintf("00-%s-%s-%s", parentSpanContext.TraceID(), workflowSpanID, parentSpanContext.TraceFlags()),
	}
	if traceState := parentSpanContext.TraceState().String(); traceState != "" {
		workflowTraceContext.TraceState = wrapperspb.String(traceState)
	}
	setPendingTraceContext(s, workflowTraceContext)
	return nil
}

// cloneExecutionStartedEvent replaces the shared source-history event with a
// copy before changing trace fields, so building a fork does not mutate the
// source instance's in-memory history.
func cloneExecutionStartedEvent(s *state.State) *protos.ExecutionStartedEvent {
	for i, event := range s.History {
		if event.GetExecutionStarted() != nil {
			cloned := proto.Clone(event).(*protos.HistoryEvent)
			s.History[i] = cloned
			return cloned.GetExecutionStarted()
		}
	}
	for i, event := range s.Inbox {
		if event.GetExecutionStarted() != nil {
			cloned := proto.Clone(event).(*protos.HistoryEvent)
			s.Inbox[i] = cloned
			return cloned.GetExecutionStarted()
		}
	}
	return nil
}

// setPendingTraceContext updates only work that will be re-dispatched by the
// rerun. Historical completed tasks intentionally retain their original
// trace metadata.
func setPendingTraceContext(s *state.State, traceContext *protos.TraceContext) {
	for i, event := range s.Inbox {
		if event == nil || (event.GetTaskScheduled() == nil && event.GetChildWorkflowInstanceCreated() == nil) {
			continue
		}

		cloned := proto.Clone(event).(*protos.HistoryEvent)
		if task := cloned.GetTaskScheduled(); task != nil {
			task.ParentTraceContext = traceContext
		} else if child := cloned.GetChildWorkflowInstanceCreated(); child != nil {
			child.ParentTraceContext = traceContext
		}
		s.Inbox[i] = cloned
	}
}

// newWorkflowSpanID returns a non-zero, randomly generated W3C span ID.
func newWorkflowSpanID() (string, error) {
	var id [8]byte
	for {
		if _, err := rand.Read(id[:]); err != nil {
			return "", fmt.Errorf("failed to generate workflow span ID: %w", err)
		}
		if id != [8]byte{} {
			return hex.EncodeToString(id[:]), nil
		}
	}
}
