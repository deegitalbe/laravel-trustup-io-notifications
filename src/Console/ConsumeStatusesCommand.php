<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Console;

use Deegitalbe\TrustupIoNotificationsClient\Kafka\Handlers\ConsumeStatusHandler;
use Deegitalbe\TrustupIoNotificationsContracts\Kafka\KafkaFactory;
use Illuminate\Console\Command;
use RuntimeException;

class ConsumeStatusesCommand extends Command
{
    protected $signature = 'trustup-io-notifications:consume-statuses';

    protected $description = 'Consume inbound notification status events from Kafka.';

    public function __construct(
        private readonly ConsumeStatusHandler $handler,
        private readonly KafkaFactory $kafkaFactory,
    ) {
        parent::__construct();
    }

    public function handle(): void
    {
        $consumer = $this->kafkaFactory->consumer([config('trustup-io-notifications-contracts.topics.status')])
            ->withManualCommit()
            ->withDlq(config('trustup-io-notifications-contracts.topics.dlq'))
            ->withHandler($this->handler)
            ->onStopConsuming(function (): void {
                report(new RuntimeException('trustup-io-notifications:consume-statuses stopped consuming.'));
            })
            ->build();

        $consumer->consume();
    }
}
