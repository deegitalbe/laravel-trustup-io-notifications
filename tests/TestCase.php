<?php

declare(strict_types=1);

namespace Deegitalbe\TrustupIoNotificationsClient\Tests;

use Deegitalbe\TrustupIoNotificationsClient\TrustupIoNotificationsClientServiceProvider;
use Deegitalbe\TrustupIoNotificationsContracts\TrustupIoNotificationsContractsServiceProvider;
use Junges\Kafka\Providers\LaravelKafkaServiceProvider;
use Orchestra\Testbench\TestCase as Orchestra;

class TestCase extends Orchestra
{
    protected function getPackageProviders($app): array
    {
        return [
            LaravelKafkaServiceProvider::class,
            TrustupIoNotificationsContractsServiceProvider::class,
            TrustupIoNotificationsClientServiceProvider::class,
        ];
    }

    protected function getEnvironmentSetUp($app): void
    {
        $app['config']->set('kafka.brokers', 'localhost:9092');
        $app['config']->set('kafka.securityProtocol', 'PLAINTEXT');
        $app['config']->set('kafka.auto_commit', false);
    }
}
