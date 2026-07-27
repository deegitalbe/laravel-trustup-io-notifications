<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Actions;

use Deegitalbe\TrustupIoNotificationsClient\Exceptions\MissingTopicConfigException;
use Deegitalbe\TrustupIoNotificationsContracts\Envelope;
use Deegitalbe\TrustupIoNotificationsContracts\Serialization\EnvelopeSerializer;
use Junges\Kafka\Facades\Kafka;
use Junges\Kafka\Message\Message;

class PublishNotificationRequest
{
    public function __construct(private readonly EnvelopeSerializer $serializer) {}

    public function execute(Envelope $envelope): void
    {
        $topic = config('trustup-io-notifications.topics.request');

        if (! is_string($topic) || $topic === '') {
            throw new MissingTopicConfigException('trustup-io-notifications.topics.request');
        }

        $encoded = $this->serializer->encode($envelope);

        Kafka::publish()
            ->onTopic($topic)
            ->withMessage(Message::create()->withBody($encoded))
            ->send();
    }
}
