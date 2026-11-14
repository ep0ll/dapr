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

package streaming

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"

	contribpubsub "github.com/dapr/components-contrib/pubsub"
	"github.com/dapr/dapr/pkg/config"
	diag "github.com/dapr/dapr/pkg/diagnostics"
	rtv1pb "github.com/dapr/dapr/pkg/proto/runtime/v1"
	rterrors "github.com/dapr/dapr/pkg/runtime/errors"
	"github.com/dapr/dapr/pkg/runtime/pubsub"
	"github.com/dapr/dapr/pkg/runtime/subscription/postman"
	"github.com/dapr/dapr/pkg/runtime/subscription/todo"
	"github.com/dapr/kit/logger"
)

var log = logger.NewLogger("dapr.runtime.processor.pubsub.subscription.streaming")

const deadLetterPublishTimeout = 30 * time.Second

type Options struct {
	Channel pubsub.AdapterStreamer
	Adapter pubsub.Adapter
	Tracing *config.TracingSpec
}

type streaming struct {
	channel     pubsub.AdapterStreamer
	adapter     pubsub.Adapter
	tracingSpec *config.TracingSpec
}

func New(opts Options) postman.Interface {
	return &streaming{
		channel:     opts.Channel,
		adapter:     opts.Adapter,
		tracingSpec: opts.Tracing,
	}
}

func (s *streaming) Deliver(ctx context.Context, msg *pubsub.SubscribedMessage) error {
	start := time.Now()
	// TODO: @joshvanl: move diagnostics handling to this package.
	resp, err := s.channel.Publish(ctx, msg)
	if err != nil {
		return err
	}

	elapsed := diag.ElapsedSince(start)

	switch resp.GetStatus().GetStatus() {
	case rtv1pb.TopicEventResponse_SUCCESS: //nolint:nosnakecase
		// on uninitialized status, this is the case it defaults to as an uninitialized status defaults to 0 which is
		// success from protobuf definition
		diag.DefaultComponentMonitoring.PubsubIngressEvent(ctx, msg.PubSub, strings.ToLower(string(contribpubsub.Success)), "", msg.Topic, elapsed)
		return nil
	case rtv1pb.TopicEventResponse_RETRY: //nolint:nosnakecase
		diag.DefaultComponentMonitoring.PubsubIngressEvent(ctx, msg.PubSub, strings.ToLower(string(contribpubsub.Retry)), "", msg.Topic, elapsed)
		// TODO: add retry error info
		return fmt.Errorf("RETRY status returned from app while processing pub/sub event %v: %w", msg.CloudEvent[contribpubsub.IDField], rterrors.NewRetriable(nil))
	case rtv1pb.TopicEventResponse_DROP: //nolint:nosnakecase
		log.Warnf("DROP status returned from app while processing pub/sub event %v", msg.CloudEvent[contribpubsub.IDField])
		diag.DefaultComponentMonitoring.PubsubIngressEvent(ctx, msg.PubSub, strings.ToLower(string(contribpubsub.Drop)), "", msg.Topic, elapsed)

		return pubsub.ErrMessageDropped
	default:
		// Consider unknown status field as error and retry
		diag.DefaultComponentMonitoring.PubsubIngressEvent(ctx, msg.PubSub, strings.ToLower(string(contribpubsub.Retry)), "", msg.Topic, elapsed)
		return fmt.Errorf("unknown status returned from app while processing pub/sub event %v, status: %v, err: %w", msg.CloudEvent[contribpubsub.IDField], resp.GetStatus(), rterrors.NewRetriable(nil))
	}
}

// DeliverBulk publishes a batch over the streaming connection and processes each
// acknowledgement independently, preserving the bulk subscription retry contract.
func (s *streaming) DeliverBulk(ctx context.Context, req *postman.DeliverBulkRequest) error {
	bscData := *req.BulkSubCallData
	psm := req.BulkSubMsg
	bulkResponses := req.BulkResponses

	type pendingEvent struct {
		envelope *rtv1pb.TopicEventRequest
		message  todo.Message
	}

	pending := make([]pendingEvent, 0, len(psm.PubSubMessages))
	spans := make([]trace.Span, 0, len(psm.PubSubMessages))
	defer todo.EndSpans(spans)

	var preparationErr error
	for _, message := range psm.PubSubMessages {
		if message.Entry == nil {
			err := errors.New("bulk streaming message is missing its original entry")
			todo.AddBulkResponseEntry(bulkResponses, "", err)
			bscData.BulkSubDiag.AddStatusCount(contribpubsub.Retry, 1)
			preparationErr = errors.Join(preparationErr, err)
			continue
		}

		var envelope *rtv1pb.TopicEventRequest
		if req.RawPayload {
			envelope = &rtv1pb.TopicEventRequest{
				Id:              message.Entry.EntryId,
				Data:            message.Entry.Event,
				DataContentType: message.Entry.ContentType,
				Topic:           psm.Topic,
				PubsubName:      psm.Pubsub,
				Path:            psm.Path,
			}
		} else {
			subscribedMessage := &pubsub.SubscribedMessage{
				CloudEvent:   message.CloudEvent,
				Topic:        psm.Topic,
				Metadata:     psm.Metadata,
				Path:         psm.Path,
				PubSub:       psm.Pubsub,
				SubscriberID: bscData.SubscriberID,
			}
			_, envelope, span, err := pubsub.GRPCEnvelopeFromSubscriptionMessage(ctx, subscribedMessage, log, s.tracingSpec)
			if span != nil {
				spans = append(spans, span)
			}
			if err != nil {
				todo.AddBulkResponseEntry(bulkResponses, message.Entry.EntryId, err)
				bscData.BulkSubDiag.AddStatusCount(contribpubsub.Retry, 1)
				preparationErr = errors.Join(preparationErr, err)
				continue
			}
			// The stream ACK protocol correlates responses by ID. Use the component
			// entry ID only when the CloudEvent does not provide one.
			if envelope.GetId() == "" {
				envelope.Id = message.Entry.EntryId
			}
		}

		pending = append(pending, pendingEvent{envelope: envelope, message: message})
	}

	// An ACK carries only the event ID. Do not send ambiguous batches where
	// multiple entries would be indistinguishable to the receiver.
	byID := make(map[string][]pendingEvent, len(pending))
	for _, item := range pending {
		byID[item.envelope.GetId()] = append(byID[item.envelope.GetId()], item)
	}
	events := make([]*rtv1pb.TopicEventRequest, 0, len(pending))
	attemptMessages := make(map[string]todo.Message, len(pending))
	seen := make(map[string]bool, len(byID))
	for _, item := range pending {
		id := item.envelope.GetId()
		if seen[id] {
			continue
		}
		seen[id] = true
		group := byID[id]
		if len(group) > 1 {
			err := fmt.Errorf("duplicate event ID %q in bulk streaming subscription", id)
			for _, duplicate := range group {
				todo.AddBulkResponseEntry(bulkResponses, duplicate.message.Entry.EntryId, err)
				bscData.BulkSubDiag.AddStatusCount(contribpubsub.Retry, 1)
			}
			preparationErr = errors.Join(preparationErr, err)
			continue
		}
		attemptMessages[id] = item.message
		events = append(events, item.envelope)
	}

	var (
		acknowledgements map[string]*rtv1pb.SubscribeTopicEventsRequestProcessedAlpha1
		publishErr       error
	)
	start := time.Now()
	if len(events) > 0 {
		acknowledgements, publishErr = s.channel.PublishBulk(ctx, &pubsub.SubscribedMessage{
			PubSub:       bscData.PsName,
			Topic:        psm.Topic,
			SubscriberID: bscData.SubscriberID,
		}, events)
	}
	elapsed := diag.ElapsedSince(start)

	for _, span := range spans {
		attributes := diag.ConstructSubscriptionSpanAttributes(psm.Topic)
		diag.AddAttributesToSpan(span, attributes)
		diag.UpdateSpanStatusFromGRPCError(span, publishErr)
	}

	hasAnyError := preparationErr != nil
	for _, event := range events {
		message := attemptMessages[event.GetId()]
		response := acknowledgements[event.GetId()]
		if response == nil {
			err := publishErr
			if err == nil {
				err = fmt.Errorf("response not received, RETRY required while processing streaming bulk subscription event for entry ID %q", message.Entry.EntryId)
			}
			todo.AddBulkResponseEntry(bulkResponses, message.Entry.EntryId, err)
			bscData.BulkSubDiag.AddStatusCount(contribpubsub.Retry, 1)
			hasAnyError = true
			continue
		}

		switch response.GetStatus().GetStatus() {
		case rtv1pb.TopicEventResponse_SUCCESS: //nolint:nosnakecase
			bscData.BulkSubDiag.AddStatusCount(contribpubsub.Success, 1)
			todo.AddBulkResponseEntry(bulkResponses, message.Entry.EntryId, nil)
		case rtv1pb.TopicEventResponse_RETRY: //nolint:nosnakecase
			err := fmt.Errorf("RETRY status returned from app while processing streaming bulk pub/sub event for entry ID %q", message.Entry.EntryId)
			bscData.BulkSubDiag.AddStatusCount(contribpubsub.Retry, 1)
			todo.AddBulkResponseEntry(bulkResponses, message.Entry.EntryId, err)
			hasAnyError = true
		case rtv1pb.TopicEventResponse_DROP: //nolint:nosnakecase
			bscData.BulkSubDiag.AddStatusCount(contribpubsub.Drop, 1)
			todo.AddBulkResponseEntry(bulkResponses, message.Entry.EntryId, nil)
			if req.DeadLetterTopic != "" {
				if err := s.sendToDeadLetter(ctx, message, psm, req.DeadLetterTopic); err != nil {
					log.Errorf("failed to send dropped bulk streaming pub/sub event to dead letter topic %s: %s", req.DeadLetterTopic, err)
				}
			}
		default:
			err := fmt.Errorf("unknown status returned from app while processing streaming bulk pub/sub event for entry ID %q: %v", message.Entry.EntryId, response.GetStatus())
			bscData.BulkSubDiag.AddStatusCount(contribpubsub.Retry, 1)
			todo.AddBulkResponseEntry(bulkResponses, message.Entry.EntryId, err)
			hasAnyError = true
		}
	}

	bscData.BulkSubDiag.SetElapsed(elapsed)
	if hasAnyError {
		return errors.Join(preparationErr, publishErr, errors.New("one or more messages failed during streaming bulk subscription delivery"))
	}
	return nil
}

func (s *streaming) sendToDeadLetter(
	ctx context.Context,
	message todo.Message,
	psm *todo.BulkSubscribedMessage,
	deadLetterTopic string,
) error {
	if s.adapter == nil {
		return errors.New("pubsub adapter is not configured for streaming dead-letter delivery")
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return ctx.Err()
	}

	contentType := message.Entry.ContentType
	req := &contribpubsub.PublishRequest{
		Data:        message.Entry.Event,
		PubsubName:  psm.Pubsub,
		Topic:       deadLetterTopic,
		Metadata:    message.Entry.Metadata,
		ContentType: &contentType,
	}
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deadLetterPublishTimeout)
	defer cancel()

	if err := s.adapter.Publish(publishCtx, req, pubsub.TransportModeGRPC); err != nil {
		return fmt.Errorf("error sending message to dead letter topic %s: %w", deadLetterTopic, err)
	}
	return nil
}

