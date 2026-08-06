<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Actions;

use Deegitalbe\TrustupIoNotificationsClient\Exceptions\MissingTopicConfigException;
use Deegitalbe\TrustupIoNotificationsContracts\Envelope;
use Deegitalbe\TrustupIoNotificationsContracts\Kafka\KafkaFactory;
use Deegitalbe\TrustupIoNotificationsContracts\Serialization\EnvelopeSerializer;
use Junges\Kafka\Message\Message;

class PublishNotificationRequest
{
    public function __construct(
        private readonly EnvelopeSerializer $serializer,
        private readonly KafkaFactory $kafkaFactory,
    ) {}

    public function execute(Envelope $envelope): void
    {
        $topic = config('trustup-io-notifications-contracts.topics.request');

        if (! is_string($topic) || $topic === '') {
            throw new MissingTopicConfigException('trustup-io-notifications-contracts.topics.request');
        }

        $encoded = $this->serializer->encode($envelope);

        $this->kafkaFactory->producer()
            ->onTopic($topic)
            ->withMessage(Message::create()->withBody($encoded))
            ->send();
    }
}
