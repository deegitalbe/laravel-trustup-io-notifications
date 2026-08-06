<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Console;

use Deegitalbe\TrustupIoNotificationsClient\Kafka\Handlers\ConsumeEngagementHandler;
use Deegitalbe\TrustupIoNotificationsContracts\Kafka\KafkaFactory;
use Illuminate\Console\Command;
use RuntimeException;

class ConsumeEngagementsCommand extends Command
{
    protected $signature = 'trustup-io-notifications:consume-engagements';

    protected $description = 'Consume inbound notification engagement events from Kafka.';

    public function __construct(
        private readonly ConsumeEngagementHandler $handler,
        private readonly KafkaFactory $kafkaFactory,
    ) {
        parent::__construct();
    }

    public function handle(): void
    {
        $consumer = $this->kafkaFactory->consumer([config('trustup-io-notifications-contracts.topics.engagement')])
            ->withManualCommit()
            ->withDlq(config('trustup-io-notifications-contracts.topics.dlq'))
            ->withHandler($this->handler)
            ->onStopConsuming(function (): void {
                report(new RuntimeException('trustup-io-notifications:consume-engagements stopped consuming.'));
            })
            ->build();

        $consumer->consume();
    }
}
