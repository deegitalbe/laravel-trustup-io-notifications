<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Console;

use Deegitalbe\TrustupIoNotificationsClient\Kafka\Handlers\ConsumeEngagementHandler;
use Illuminate\Console\Command;
use Junges\Kafka\Facades\Kafka;
use RuntimeException;

class ConsumeEngagementsCommand extends Command
{
    protected $signature = 'trustup-io-notifications:consume-engagements';

    protected $description = 'Consume inbound notification engagement events from Kafka.';

    public function __construct(private readonly ConsumeEngagementHandler $handler)
    {
        parent::__construct();
    }

    public function handle(): void
    {
        $consumer = Kafka::consumer([config('trustup-io-notifications.topics.engagement')])
            ->withManualCommit()
            ->withDlq(config('trustup-io-notifications.topics.dlq'))
            ->withHandler($this->handler)
            ->onStopConsuming(function (): void {
                report(new RuntimeException('trustup-io-notifications:consume-engagements stopped consuming.'));
            })
            ->build();

        $consumer->consume();
    }
}
