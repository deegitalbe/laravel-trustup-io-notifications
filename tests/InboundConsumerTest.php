<?php

declare(strict_types=1);

use Deegitalbe\TrustupIoNotificationsClient\Console\ConsumeEngagementsCommand;
use Deegitalbe\TrustupIoNotificationsClient\Console\ConsumeStatusesCommand;
use Deegitalbe\TrustupIoNotificationsClient\Events\TrustupIoNotificationEngagementReceived;
use Deegitalbe\TrustupIoNotificationsClient\Events\TrustupIoNotificationStatusReceived;
use Deegitalbe\TrustupIoNotificationsClient\Jobs\HandleNotificationEngagementJob;
use Deegitalbe\TrustupIoNotificationsClient\Jobs\HandleNotificationStatusJob;
use Deegitalbe\TrustupIoNotificationsClient\Kafka\Handlers\ConsumeEngagementHandler;
use Deegitalbe\TrustupIoNotificationsClient\Kafka\Handlers\ConsumeStatusHandler;
use Deegitalbe\TrustupIoNotificationsContracts\Data\ToolsTestNotificationData;
use Deegitalbe\TrustupIoNotificationsContracts\Engagement\EngagementPayload;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\ChannelEventKind;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\EventDirection;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\NotificationChannel;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\NotificationStatus;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\NotificationType;
use Deegitalbe\TrustupIoNotificationsContracts\Enums\Source;
use Deegitalbe\TrustupIoNotificationsContracts\Envelope;
use Deegitalbe\TrustupIoNotificationsContracts\Request\Recipient;
use Deegitalbe\TrustupIoNotificationsContracts\Request\RequestPayload;
use Deegitalbe\TrustupIoNotificationsContracts\Serialization\EnvelopeSerializer;
use Deegitalbe\TrustupIoNotificationsContracts\Status\StatusPayload;
use Illuminate\Support\Facades\Event;
use Illuminate\Support\Facades\Queue;
use Junges\Kafka\Contracts\ConsumerMessage;
use Junges\Kafka\Contracts\MessageConsumer;
use Junges\Kafka\Facades\Kafka;

// -------------------------------------------------------------------------
// Helpers: build a valid encoded status envelope body
// -------------------------------------------------------------------------

/** @return array<string, mixed> */
function makeStatusEnvelopeBody(
    string $sendId = 'send-abc-123',
    NotificationChannel $channel = NotificationChannel::Email,
    NotificationStatus $status = NotificationStatus::Sent,
    NotificationType $type = NotificationType::ToolsTestNotification,
    string $title = 'Test Title',
    string $body = 'Test Body',
): array {
    $payload = new StatusPayload(
        sendId: $sendId,
        channel: $channel,
        status: $status,
        type: $type,
        data: $type->dataClass()::fromArray(['base_url' => 'https://example.test', 'title' => $title, 'body' => $body]),
    );

    $envelope = new Envelope(
        version: Envelope::CURRENT_VERSION,
        direction: EventDirection::Status,
        payload: $payload,
    );

    return app(EnvelopeSerializer::class)->encode($envelope);
}

/** @param array<string, mixed> $body */
function makeConsumerMessage(array $body): ConsumerMessage
{
    return new class($body) implements ConsumerMessage
    {
        /** @param array<string, mixed> $body */
        public function __construct(private readonly array $body) {}

        public function getBody(): array
        {
            return $this->body;
        }

        public function getKey(): ?string
        {
            return null;
        }

        public function getHeaders(): array
        {
            return [];
        }

        public function getTopicName(): string
        {
            return 'notifications.status';
        }

        public function getPartition(): int
        {
            return 0;
        }

        public function getOffset(): int
        {
            return 0;
        }

        public function getTimestamp(): int
        {
            return time();
        }

        public function getPayload(): mixed
        {
            return $this->body;
        }

        public function getMessageIdentifier(): string
        {
            return 'test-message-id';
        }
    };
}

function makeMessageConsumerSpy(): MessageConsumer
{
    return new class implements MessageConsumer
    {
        public int $commitCount = 0;

        public function consume(): void {}

        public function stopConsuming(): void {}

        public function cancelStopConsume(): void {}

        public function consumedMessagesCount(): int
        {
            return 0;
        }

        public function commit(mixed $messageOrOffsets = null): void
        {
            $this->commitCount++;
        }

        public function commitAsync(mixed $message_or_offsets = null): void {}

        public function getAssignedPartitions(): array
        {
            return [];
        }
    };
}

// -------------------------------------------------------------------------
// Cycle 12: Nominal inbound consumer (AC-12, AC-13)
// -------------------------------------------------------------------------
it('dispatches HandleNotificationStatusJob with decoded StatusPayload and commits offset when valid status envelope is consumed', function (): void {
    Queue::fake();

    $body = makeStatusEnvelopeBody();
    $message = makeConsumerMessage($body);
    $consumer = makeMessageConsumerSpy();

    app(ConsumeStatusHandler::class)($message, $consumer);

    Queue::assertPushed(HandleNotificationStatusJob::class, function (HandleNotificationStatusJob $job): bool {
        return $job->payload->sendId === 'send-abc-123'
            && $job->payload->channel === NotificationChannel::Email
            && $job->payload->status === NotificationStatus::Sent
            && $job->payload->type === NotificationType::ToolsTestNotification;
    });

    expect($consumer->commitCount)->toBe(1);
});

// -------------------------------------------------------------------------
// Cycle 13: Job fires event (AC-14, AC-15, AC-16)
// -------------------------------------------------------------------------
it('dispatches TrustupIoNotificationStatusReceived event carrying the full StatusPayload when HandleNotificationStatusJob runs', function (): void {
    Event::fake();

    $payload = new StatusPayload(
        sendId: 'send-xyz-456',
        channel: NotificationChannel::Sms,
        status: NotificationStatus::Error,
        type: NotificationType::ToolsTestNotification,
        data: NotificationType::ToolsTestNotification->dataClass()::fromArray(['base_url' => 'https://example.test', 'title' => 'Hi', 'body' => 'Body']),
    );

    $job = new HandleNotificationStatusJob($payload);
    $job->handle();

    Event::assertDispatched(TrustupIoNotificationStatusReceived::class, function (TrustupIoNotificationStatusReceived $event) use ($payload): bool {
        return $event->payload->sendId === $payload->sendId
            && $event->payload->channel === $payload->channel
            && $event->payload->status === $payload->status
            && $event->payload->type === $payload->type;
    });
});

// -------------------------------------------------------------------------
// Cycle 14: No listener is a no-op (AC-14)
// -------------------------------------------------------------------------
it('dispatches the status event with no listeners registered without error', function (): void {
    $payload = new StatusPayload(
        sendId: 'send-no-listener',
        channel: NotificationChannel::Push,
        status: NotificationStatus::Pending,
        type: NotificationType::ToolsTestNotification,
        data: NotificationType::ToolsTestNotification->dataClass()::fromArray(['base_url' => 'https://example.test', 'title' => 'Hi', 'body' => 'Body']),
    );

    $job = new HandleNotificationStatusJob($payload);

    expect(fn () => $job->handle())->not->toThrow(Throwable::class);
});

// -------------------------------------------------------------------------
// Cycle 15: Undecodable / unknown version message is skipped (AC-12)
// -------------------------------------------------------------------------
it('skips message and commits offset without dispatching a job when envelope body is malformed', function (): void {
    Queue::fake();

    $message = makeConsumerMessage(['malformed' => true, 'no_version_key' => 'at all']);
    $consumer = makeMessageConsumerSpy();

    app(ConsumeStatusHandler::class)($message, $consumer);

    Queue::assertNothingPushed();
    expect($consumer->commitCount)->toBe(1);
});

it('skips message and commits offset without dispatching a job when envelope version is unknown', function (): void {
    Queue::fake();

    $body = makeStatusEnvelopeBody();
    $body['version'] = 999;

    $message = makeConsumerMessage($body);
    $consumer = makeMessageConsumerSpy();

    app(ConsumeStatusHandler::class)($message, $consumer);

    Queue::assertNothingPushed();
    expect($consumer->commitCount)->toBe(1);
});

// -------------------------------------------------------------------------
// Cycle 23: Misrouted request-direction envelope is skipped (Blocker 2 - instanceof guard)
// -------------------------------------------------------------------------
it('skips message and commits offset without dispatching a job when envelope payload is not a StatusPayload', function (): void {
    Queue::fake();

    $requestPayload = new RequestPayload(
        type: NotificationType::ToolsTestNotification,
        recipient: Recipient::identified('user-42', Source::Tools),
        data: new ToolsTestNotificationData('https://example.test', 'Title', 'Body'),
        channels: null,
    );

    $requestEnvelope = new Envelope(
        version: Envelope::CURRENT_VERSION,
        direction: EventDirection::Request,
        payload: $requestPayload,
    );

    $body = app(EnvelopeSerializer::class)->encode($requestEnvelope);
    $message = makeConsumerMessage($body);
    $consumer = makeMessageConsumerSpy();

    app(ConsumeStatusHandler::class)($message, $consumer);

    Queue::assertNothingPushed();
    expect($consumer->commitCount)->toBe(1);
});

// -------------------------------------------------------------------------
// Cycle 16: Dispatch failure does not commit offset (AC-13)
// -------------------------------------------------------------------------
it('does not commit offset when job dispatch throws', function (): void {
    $body = makeStatusEnvelopeBody();
    $message = makeConsumerMessage($body);
    $consumer = makeMessageConsumerSpy();

    // Bind a dispatcher that throws to simulate dispatch failure
    $this->app->bind(
        Illuminate\Contracts\Bus\Dispatcher::class,
        fn () => new class implements Illuminate\Contracts\Bus\Dispatcher
        {
            public function dispatch($command): mixed
            {
                throw new RuntimeException('Simulated dispatch failure');
            }

            public function dispatchSync($command, $handler = null): mixed
            {
                throw new RuntimeException('Simulated dispatch failure');
            }

            public function dispatchNow($command, $handler = null): mixed
            {
                throw new RuntimeException('Simulated dispatch failure');
            }

            public function hasCommandHandler($command): bool
            {
                return false;
            }

            public function getCommandHandler($command): bool|object
            {
                return false;
            }

            /** @param array<int, class-string> $pipes */
            public function pipeThrough(array $pipes): static
            {
                return $this;
            }

            /** @param array<class-string, class-string> $map */
            public function map(array $map): static
            {
                return $this;
            }
        },
    );

    expect(fn () => app(ConsumeStatusHandler::class)($message, $consumer))
        ->toThrow(RuntimeException::class);

    expect($consumer->commitCount)->toBe(0);
});

// -------------------------------------------------------------------------
// Cycle 21: ConsumeStatusesCommand resolves and has ConsumeStatusHandler injected (command coverage)
// -------------------------------------------------------------------------
it('trustup-io-notifications:consume-statuses command runs without error when no messages are available', function (): void {
    Kafka::fake();

    $this->artisan('trustup-io-notifications:consume-statuses')->assertSuccessful();
});

it('ConsumeStatusesCommand is resolvable from the container with ConsumeStatusHandler injected', function (): void {
    $command = app(ConsumeStatusesCommand::class);

    expect($command)->toBeInstanceOf(ConsumeStatusesCommand::class);
});

// -------------------------------------------------------------------------
// Engagement consumer: AC-15, AC-16, AC-17
// -------------------------------------------------------------------------

/** @return array<string, mixed> */
function makeEngagementEnvelopeBody(
    string $sendId = 'send-eng-123',
    NotificationChannel $channel = NotificationChannel::Email,
    ChannelEventKind $kind = ChannelEventKind::Opened,
    NotificationType $type = NotificationType::ToolsTestNotification,
    ?string $clickedUrl = null,
): array {
    $payload = new EngagementPayload(
        sendId: $sendId,
        channel: $channel,
        kind: $kind,
        type: $type,
        data: $type->dataClass()::fromArray(['base_url' => 'https://example.test', 'title' => 'T', 'body' => 'B']),
        clickedUrl: $clickedUrl,
    );

    $envelope = new Envelope(
        version: Envelope::CURRENT_VERSION,
        direction: EventDirection::Engagement,
        payload: $payload,
    );

    return app(EnvelopeSerializer::class)->encode($envelope);
}

// AC-15 nominal: valid engagement envelope dispatches job and fires event
it('AC-15 dispatches HandleNotificationEngagementJob with decoded EngagementPayload when valid engagement envelope is consumed', function (): void {
    Queue::fake();

    $body = makeEngagementEnvelopeBody('send-eng-001', NotificationChannel::Email, ChannelEventKind::Opened);
    $message = makeConsumerMessage($body);
    $consumer = makeMessageConsumerSpy();

    app(ConsumeEngagementHandler::class)($message, $consumer);

    Queue::assertPushed(HandleNotificationEngagementJob::class, function (HandleNotificationEngagementJob $job): bool {
        return $job->payload->sendId === 'send-eng-001'
            && $job->payload->channel === NotificationChannel::Email
            && $job->payload->kind === ChannelEventKind::Opened;
    });

    expect($consumer->commitCount)->toBe(1);
});

it('AC-15 skips and commits offset without dispatching when the engagement envelope body is malformed', function (): void {
    Queue::fake();

    $message = makeConsumerMessage(['malformed' => true, 'no_version_key' => 'at all']);
    $consumer = makeMessageConsumerSpy();

    app(ConsumeEngagementHandler::class)($message, $consumer);

    Queue::assertNothingPushed();
    expect($consumer->commitCount)->toBe(1);
});

it('AC-15 skips and commits offset without dispatching when the engagement envelope version is unknown', function (): void {
    Queue::fake();

    $body = makeEngagementEnvelopeBody('send-eng-bad-version', NotificationChannel::Email, ChannelEventKind::Opened);
    $body['version'] = 999;

    $message = makeConsumerMessage($body);
    $consumer = makeMessageConsumerSpy();

    app(ConsumeEngagementHandler::class)($message, $consumer);

    Queue::assertNothingPushed();
    expect($consumer->commitCount)->toBe(1);
});

// AC-17 detail: job fires event with all payload fields including clickedUrl
it('AC-17 HandleNotificationEngagementJob fires TrustupIoNotificationEngagementReceived with full EngagementPayload', function (): void {
    Event::fake();

    $payload = new EngagementPayload(
        sendId: 'send-eng-click',
        channel: NotificationChannel::Email,
        kind: ChannelEventKind::Clicked,
        type: NotificationType::ToolsTestNotification,
        data: NotificationType::ToolsTestNotification->dataClass()::fromArray(['base_url' => 'https://example.test', 'title' => 'T', 'body' => 'B']),
        clickedUrl: 'https://example.com/link',
    );

    $job = new HandleNotificationEngagementJob($payload);
    $job->handle();

    Event::assertDispatched(TrustupIoNotificationEngagementReceived::class, function (TrustupIoNotificationEngagementReceived $event) use ($payload): bool {
        return $event->payload->sendId === $payload->sendId
            && $event->payload->channel === $payload->channel
            && $event->payload->kind === $payload->kind
            && $event->payload->clickedUrl === $payload->clickedUrl;
    });
});

// AC-15 misrouted: status-direction envelope on engagement consumer -> skipped + reported
it('AC-15 skips and commits offset when a non-EngagementPayload envelope is received on the engagement consumer', function (): void {
    Queue::fake();

    $body = makeStatusEnvelopeBody();
    $message = makeConsumerMessage($body);
    $consumer = makeMessageConsumerSpy();

    app(ConsumeEngagementHandler::class)($message, $consumer);

    Queue::assertNothingPushed();
    expect($consumer->commitCount)->toBe(1);
});

// AC-16 independence: both commands are independently registered and subscribe to their own topic
it('AC-16 ConsumeEngagementsCommand is resolvable from the container independently of ConsumeStatusesCommand', function (): void {
    $engCmd = app(ConsumeEngagementsCommand::class);
    $statusCmd = app(ConsumeStatusesCommand::class);

    expect($engCmd)->toBeInstanceOf(ConsumeEngagementsCommand::class);
    expect($statusCmd)->toBeInstanceOf(ConsumeStatusesCommand::class);
});

it('AC-16 consume-engagements command runs without error when no messages are available', function (): void {
    Kafka::fake();

    $this->artisan('trustup-io-notifications:consume-engagements')->assertSuccessful();
});

// AC-17 undecodable: malformed engagement message -> skipped, committed
it('AC-17 skips and commits offset without dispatching job when engagement envelope body is malformed', function (): void {
    Queue::fake();

    $message = makeConsumerMessage(['not_valid' => true]);
    $consumer = makeMessageConsumerSpy();

    app(ConsumeEngagementHandler::class)($message, $consumer);

    Queue::assertNothingPushed();
    expect($consumer->commitCount)->toBe(1);
});
