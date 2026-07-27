<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Kafka\Handlers;

use Deegitalbe\TrustupIoNotificationsClient\Jobs\HandleNotificationStatusJob;
use Deegitalbe\TrustupIoNotificationsContracts\Envelope;
use Deegitalbe\TrustupIoNotificationsContracts\Serialization\EnvelopeSerializer;
use Deegitalbe\TrustupIoNotificationsContracts\Status\StatusPayload;
use Junges\Kafka\Contracts\ConsumerMessage;
use Junges\Kafka\Contracts\Handler;
use Junges\Kafka\Contracts\MessageConsumer;
use RuntimeException;
use Throwable;

class ConsumeStatusHandler implements Handler
{
    public function __construct(private readonly EnvelopeSerializer $serializer) {}

    public function __invoke(ConsumerMessage $message, MessageConsumer $consumer): void
    {
        try {
            /** @var array<string, mixed> $body */
            $body = (array) $message->getBody();

            $envelope = $this->serializer->decode($body);
        } catch (Throwable $exception) {
            report($exception);
            $consumer->commit($message);

            return;
        }

        if ($envelope->version !== Envelope::CURRENT_VERSION) {
            report(new RuntimeException("Received status envelope with unknown version [{$envelope->version}]."));
            $consumer->commit($message);

            return;
        }

        if (! $envelope->payload instanceof StatusPayload) {
            report(new RuntimeException('Received envelope with unexpected payload type ['.get_class($envelope->payload).'] on the status topic.'));
            $consumer->commit($message);

            return;
        }

        HandleNotificationStatusJob::dispatch($envelope->payload);

        $consumer->commit($message);
    }
}
